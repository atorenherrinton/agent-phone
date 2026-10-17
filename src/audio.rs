//! Pure, bounded audio helpers for the STT -> LLM -> TTS cascade.
//!
//! Twilio media streams carry 8 kHz mono G.711 mu-law. A speech-synthesis-only
//! engine such as Kokoro returns 24 kHz signed 16-bit PCM, and speech-to-text
//! endpoints want a container such as WAV. Nothing here does I/O, logs audio, or
//! keeps unbounded state.

use std::collections::VecDeque;

pub const TELEPHONY_RATE: u32 = 8_000;
pub const TTS_RATE: u32 = 24_000;
/// 20 ms of 8 kHz audio.
pub const FRAME_SAMPLES: usize = 160;

const BIAS: i32 = 0x84;
const CLIP: i32 = 32_635;

pub fn mulaw_decode(byte: u8) -> i16 {
    let value = !byte;
    let exponent = i32::from((value >> 4) & 0x07);
    let mantissa = i32::from(value & 0x0f);
    let magnitude = (((mantissa << 3) + BIAS) << exponent) - BIAS;
    (if value & 0x80 != 0 {
        -magnitude
    } else {
        magnitude
    }) as i16
}

pub fn mulaw_encode(sample: i16) -> u8 {
    let mut value = i32::from(sample);
    let sign = if value < 0 {
        value = -value;
        0x80
    } else {
        0
    };
    value = value.min(CLIP) + BIAS;
    let mut exponent = 7;
    let mut mask = 0x4000;
    while exponent > 0 && value & mask == 0 {
        exponent -= 1;
        mask >>= 1;
    }
    let mantissa = (value >> (exponent + 3)) & 0x0f;
    !((sign | (exponent << 4) | mantissa) as u8)
}

/// Little-endian signed 16-bit PCM bytes to samples. A dangling odd byte is dropped.
pub fn pcm16_from_le(bytes: &[u8]) -> Vec<i16> {
    bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| i16::from_le_bytes(*pair))
        .collect()
}

/// A WAV container for 8 kHz mono mu-law audio, decoded to 16-bit PCM.
pub fn wav_from_mulaw(mulaw: &[u8]) -> Vec<u8> {
    let data_len = (mulaw.len() * 2) as u32;
    let mut wav = Vec::with_capacity(44 + mulaw.len() * 2);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36 + data_len).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes()); // PCM
    wav.extend_from_slice(&1u16.to_le_bytes()); // mono
    wav.extend_from_slice(&TELEPHONY_RATE.to_le_bytes());
    wav.extend_from_slice(&(TELEPHONY_RATE * 2).to_le_bytes());
    wav.extend_from_slice(&2u16.to_le_bytes());
    wav.extend_from_slice(&16u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_len.to_le_bytes());
    for byte in mulaw {
        wav.extend_from_slice(&mulaw_decode(*byte).to_le_bytes());
    }
    wav
}

const TAPS: usize = 31;

/// Streaming 3:1 decimator (24 kHz -> 8 kHz) with a windowed-sinc anti-alias
/// filter cut at 3.4 kHz. Chunk boundaries do not change the output.
pub struct Downsampler {
    taps: [f32; TAPS],
    buffer: Vec<f32>,
    position: usize,
}

impl Default for Downsampler {
    fn default() -> Self {
        let cutoff = 3_400.0 / TTS_RATE as f32;
        let centre = (TAPS - 1) as f32 / 2.0;
        let mut taps = [0.0f32; TAPS];
        for (index, tap) in taps.iter_mut().enumerate() {
            let x = index as f32 - centre;
            let sinc = if x == 0.0 {
                2.0 * cutoff
            } else {
                (2.0 * std::f32::consts::PI * cutoff * x).sin() / (std::f32::consts::PI * x)
            };
            let window =
                0.54 - 0.46 * (2.0 * std::f32::consts::PI * index as f32 / (TAPS - 1) as f32).cos();
            *tap = sinc * window;
        }
        let gain: f32 = taps.iter().sum();
        for tap in &mut taps {
            *tap /= gain;
        }
        Self {
            taps,
            buffer: vec![0.0; TAPS - 1],
            position: 0,
        }
    }
}

impl Downsampler {
    pub fn process(&mut self, input: &[i16]) -> Vec<i16> {
        self.buffer.extend(input.iter().map(|s| f32::from(*s)));
        let mut output = Vec::with_capacity(input.len() / 3 + 1);
        while self.position + TAPS <= self.buffer.len() {
            let window = &self.buffer[self.position..self.position + TAPS];
            let value: f32 = window.iter().zip(&self.taps).map(|(a, b)| a * b).sum();
            output.push(value.round().clamp(-32_768.0, 32_767.0) as i16);
            self.position += 3;
        }
        self.buffer.drain(..self.position);
        self.position = 0;
        output
    }

    /// Flush the filter tail so the last few milliseconds of an utterance are kept.
    pub fn finish(&mut self) -> Vec<i16> {
        self.process(&[0; TAPS - 1])
    }
}

/// 24 kHz PCM16-LE from a synthesizer to 8 kHz mu-law for Twilio.
pub fn pcm24k_to_mulaw(pcm_le: &[u8]) -> Vec<u8> {
    let samples = pcm16_from_le(pcm_le);
    if samples.is_empty() {
        return Vec::new();
    }
    let mut resampler = Downsampler::default();
    let mut narrow = resampler.process(&samples);
    narrow.extend(resampler.finish());
    narrow.into_iter().map(mulaw_encode).collect()
}

#[derive(Clone, Copy, Debug)]
pub struct EndpointConfig {
    /// Consecutive loud frames that begin an utterance.
    pub start_frames: usize,
    /// Consecutive quiet frames that end one (matches the 500 ms Realtime VAD).
    pub end_frames: usize,
    /// Audio kept from before detected speech (matches 300 ms of prefix padding).
    pub preroll_frames: usize,
    /// Utterances with fewer loud frames are treated as clicks and discarded.
    pub min_speech_frames: usize,
    /// Utterances are cut and submitted at this length.
    pub max_frames: usize,
    pub min_threshold: f32,
    pub floor_multiplier: f32,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            start_frames: 3,
            end_frames: 25,
            preroll_frames: 15,
            min_speech_frames: 8,
            max_frames: 1_500, // 30 seconds
            min_threshold: 450.0,
            floor_multiplier: 3.0,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum EndpointEvent {
    SpeechStarted,
    /// Speech ended. The payload is the mu-law utterance (with preroll), or `None`
    /// when it was too short to be speech.
    SpeechStopped(Option<Vec<u8>>),
}

/// Energy endpointer that stands in for a server-side VAD. It keeps at most one
/// utterance plus a short preroll in memory.
pub struct Endpointer {
    config: EndpointConfig,
    partial: Vec<u8>,
    preroll: VecDeque<Vec<u8>>,
    utterance: Vec<u8>,
    speaking: bool,
    loud_run: usize,
    quiet_run: usize,
    loud_frames: usize,
    frames: usize,
    noise_floor: f32,
}

impl Default for Endpointer {
    fn default() -> Self {
        Self::new(EndpointConfig::default())
    }
}

fn frame_rms(frame: &[u8]) -> f32 {
    let sum: f64 = frame
        .iter()
        .map(|byte| {
            let sample = f64::from(mulaw_decode(*byte));
            sample * sample
        })
        .sum();
    (sum / frame.len().max(1) as f64).sqrt() as f32
}

impl Endpointer {
    pub fn new(config: EndpointConfig) -> Self {
        Self {
            config,
            partial: Vec::new(),
            preroll: VecDeque::new(),
            utterance: Vec::new(),
            speaking: false,
            loud_run: 0,
            quiet_run: 0,
            loud_frames: 0,
            frames: 0,
            noise_floor: 60.0,
        }
    }

    pub fn speaking(&self) -> bool {
        self.speaking
    }

    fn threshold(&self) -> f32 {
        self.config
            .min_threshold
            .max(self.noise_floor * self.config.floor_multiplier)
    }

    pub fn push(&mut self, mulaw: &[u8]) -> Vec<EndpointEvent> {
        let mut events = Vec::new();
        self.partial.extend_from_slice(mulaw);
        while self.partial.len() >= FRAME_SAMPLES {
            let frame: Vec<u8> = self.partial.drain(..FRAME_SAMPLES).collect();
            self.frame(frame, &mut events);
        }
        events
    }

    fn frame(&mut self, frame: Vec<u8>, events: &mut Vec<EndpointEvent>) {
        let rms = frame_rms(&frame);
        let loud = rms >= self.threshold();
        if !self.speaking {
            if !loud {
                self.noise_floor = (0.95 * self.noise_floor + 0.05 * rms).max(20.0);
            }
            self.loud_run = if loud { self.loud_run + 1 } else { 0 };
            self.preroll.push_back(frame);
            while self.preroll.len() > self.config.preroll_frames + self.config.start_frames {
                self.preroll.pop_front();
            }
            if self.loud_run >= self.config.start_frames {
                self.speaking = true;
                self.quiet_run = 0;
                self.loud_frames = self.loud_run;
                self.frames = self.preroll.len();
                self.utterance = self.preroll.drain(..).flatten().collect();
                events.push(EndpointEvent::SpeechStarted);
            }
            return;
        }
        self.utterance.extend_from_slice(&frame);
        self.frames += 1;
        if loud {
            self.loud_frames += 1;
            self.quiet_run = 0;
        } else {
            self.quiet_run += 1;
        }
        if self.quiet_run >= self.config.end_frames || self.frames >= self.config.max_frames {
            events.push(EndpointEvent::SpeechStopped(self.finish_utterance()));
        }
    }

    fn finish_utterance(&mut self) -> Option<Vec<u8>> {
        let keep = self.loud_frames >= self.config.min_speech_frames;
        let audio = std::mem::take(&mut self.utterance);
        self.speaking = false;
        self.loud_run = 0;
        self.quiet_run = 0;
        self.loud_frames = 0;
        self.frames = 0;
        keep.then_some(audio)
    }

    /// Hangup while the caller was mid-sentence: hand back what was heard.
    pub fn flush(&mut self) -> Option<Vec<u8>> {
        if self.speaking {
            self.finish_utterance()
        } else {
            None
        }
    }
}

#[cfg(test)]
pub(crate) fn test_tone(frames: usize, amplitude: f32) -> Vec<u8> {
    (0..frames * FRAME_SAMPLES)
        .map(|n| {
            let phase = 2.0 * std::f32::consts::PI * 440.0 * n as f32 / TELEPHONY_RATE as f32;
            mulaw_encode((amplitude * phase.sin()) as i16)
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn test_silence(frames: usize) -> Vec<u8> {
    vec![0xff; frames * FRAME_SAMPLES]
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{test_silence as silence, test_tone as tone};

    #[test]
    fn mulaw_silence_and_round_trip_stay_within_quantization_error() {
        assert_eq!(mulaw_encode(0), 0xff);
        assert_eq!(mulaw_decode(0xff), 0);
        for sample in [
            -32_000i16, -8_000, -1_000, -100, -1, 1, 100, 1_000, 8_000, 32_000,
        ] {
            let decoded = i32::from(mulaw_decode(mulaw_encode(sample)));
            let error = (decoded - i32::from(sample)).abs();
            assert!(
                error <= (i32::from(sample).abs() / 16).max(8),
                "{sample} -> {decoded}"
            );
        }
        // Every mu-law byte is stable under decode/encode.
        for byte in 0..=255u8 {
            let again = mulaw_encode(mulaw_decode(byte));
            assert_eq!(mulaw_decode(again), mulaw_decode(byte));
        }
    }

    #[test]
    fn wav_header_describes_the_decoded_payload() {
        let wav = wav_from_mulaw(&[0xff; 100]);
        assert_eq!(&wav[..4], b"RIFF");
        assert_eq!(&wav[8..16], b"WAVEfmt ");
        assert_eq!(wav.len(), 44 + 200);
        assert_eq!(u32::from_le_bytes(wav[24..28].try_into().unwrap()), 8_000);
        assert_eq!(u32::from_le_bytes(wav[40..44].try_into().unwrap()), 200);
        assert_eq!(u32::from_le_bytes(wav[4..8].try_into().unwrap()), 36 + 200);
    }

    #[test]
    fn downsampler_is_three_to_one_and_independent_of_chunking() {
        let input: Vec<i16> = (0..2_400)
            .map(|n| {
                (8_000.0 * (2.0 * std::f32::consts::PI * 500.0 * n as f32 / 24_000.0).sin()) as i16
            })
            .collect();
        let mut whole = Downsampler::default();
        let mut expected = whole.process(&input);
        expected.extend(whole.finish());
        assert!(
            (expected.len() as i32 - 800).abs() <= 11,
            "{}",
            expected.len()
        );

        let mut chunked = Downsampler::default();
        let mut actual = Vec::new();
        for chunk in input.chunks(7) {
            actual.extend(chunked.process(chunk));
        }
        actual.extend(chunked.finish());
        assert_eq!(actual, expected);

        // A 500 Hz tone is well inside the passband and keeps its level.
        let peak = expected[40..760].iter().map(|s| s.abs()).max().unwrap();
        assert!((7_000..=9_000).contains(&peak), "peak {peak}");
    }

    #[test]
    fn downsampler_rejects_energy_above_the_new_nyquist() {
        // 5 kHz would alias into the 4 kHz telephone band if not filtered.
        let input: Vec<i16> = (0..2_400)
            .map(|n| {
                (10_000.0 * (2.0 * std::f32::consts::PI * 5_000.0 * n as f32 / 24_000.0).sin())
                    as i16
            })
            .collect();
        let mut resampler = Downsampler::default();
        let output = resampler.process(&input);
        let peak = output[20..].iter().map(|s| s.abs()).max().unwrap();
        assert!(peak < 1_500, "peak {peak}");
    }

    #[test]
    fn pcm24k_to_mulaw_produces_one_third_as_many_bytes() {
        let pcm: Vec<u8> = (0..2_400i16).flat_map(|n| (n * 3).to_le_bytes()).collect();
        let mulaw = pcm24k_to_mulaw(&pcm);
        assert!((mulaw.len() as i32 - 800).abs() <= 11);
        assert!(pcm24k_to_mulaw(&[1]).is_empty());
    }

    #[test]
    fn endpointer_detects_speech_with_preroll_and_ends_after_silence() {
        let mut vad = Endpointer::default();
        assert!(vad.push(&silence(20)).is_empty());
        let started = vad.push(&tone(10, 6_000.0));
        assert_eq!(started, vec![EndpointEvent::SpeechStarted]);
        assert!(vad.speaking());
        assert!(vad.push(&silence(24)).is_empty());
        let events = vad.push(&silence(2));
        let [EndpointEvent::SpeechStopped(Some(audio))] = events.as_slice() else {
            panic!("expected one utterance, got {events:?}");
        };
        // 10 loud + 24/25 quiet trailing frames, plus preroll before the start.
        assert!(audio.len() >= (10 + 25 + 3) * FRAME_SAMPLES);
        assert!(audio.len() <= (10 + 27 + 18) * FRAME_SAMPLES);
        assert!(!vad.speaking());
    }

    #[test]
    fn endpointer_discards_clicks_and_ignores_quiet_noise() {
        let mut vad = Endpointer::default();
        assert!(vad.push(&tone(200, 150.0)).is_empty(), "line noise");
        let mut events = vad.push(&tone(4, 6_000.0));
        events.extend(vad.push(&silence(40)));
        assert_eq!(
            events,
            vec![
                EndpointEvent::SpeechStarted,
                EndpointEvent::SpeechStopped(None)
            ]
        );
    }

    #[test]
    fn endpointer_accepts_arbitrary_chunk_sizes_and_bounds_utterances() {
        let mut vad = Endpointer::default();
        let audio = tone(1_600, 6_000.0);
        let mut stops = 0;
        for chunk in audio.chunks(37) {
            for event in vad.push(chunk) {
                if let EndpointEvent::SpeechStopped(Some(utterance)) = event {
                    assert!(utterance.len() <= (1_500 + 18) * FRAME_SAMPLES);
                    stops += 1;
                }
            }
        }
        assert!(stops >= 1, "a continuous speaker is cut at the bound");
    }

    #[test]
    fn flush_returns_a_partial_utterance_only_while_speaking() {
        let mut vad = Endpointer::default();
        assert!(vad.flush().is_none());
        vad.push(&tone(12, 6_000.0));
        assert!(vad.flush().is_some());
        assert!(!vad.speaking());
    }
}
