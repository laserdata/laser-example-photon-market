use laser_sdk::prelude::LaserError;
use laser_sdk::prelude::full::{Embedder, MemoryItem, Reranker};
use std::cmp::Ordering;
use std::collections::BTreeSet;

const DIM: usize = 64;

// A deterministic token-hash embedder: same text always yields the same vector,
// so recall is reproducible and needs no network or model. Tokens hash (FNV-1a)
// into a fixed-dimension bag, then the vector is L2-normalized for cosine recall.
#[derive(Clone, Copy, Default)]
pub struct DeterministicEmbedder;

impl Embedder for DeterministicEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, LaserError> {
        Ok(embed_tokens(text))
    }
}

#[derive(Clone, Copy, Default)]
pub struct DeterministicReranker;

impl Reranker for DeterministicReranker {
    async fn rerank(
        &self,
        query: &str,
        mut items: Vec<MemoryItem>,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        for item in &mut items {
            let body = String::from_utf8_lossy(&item.payload);
            item.score = Some(overlap_score(query, &body));
        }
        items.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| right.id.cmp(&left.id))
        });
        Ok(items)
    }
}

fn embed_tokens(text: &str) -> Vec<f32> {
    let mut vector = vec![0f32; DIM];
    for token in text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
    {
        let slot = (fnv1a(&token.to_ascii_lowercase()) % DIM as u64) as usize;
        vector[slot] += 1.0;
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut vector {
            *value /= norm;
        }
    }
    vector
}

fn fnv1a(text: &str) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn overlap_score(query: &str, text: &str) -> f32 {
    let query = tokens(query);
    if query.is_empty() {
        return 0.0;
    }
    let text = tokens(text);
    let matches = query.intersection(&text).count();
    matches as f32 / query.len() as f32
}

fn tokens(text: &str) -> BTreeSet<String> {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn given_the_same_text_when_embedded_then_should_be_identical() {
        let embedder = DeterministicEmbedder;
        assert_eq!(
            embedder
                .embed("high value order device-borealis")
                .await
                .expect("embed"),
            embedder
                .embed("high value order device-borealis")
                .await
                .expect("embed"),
        );
    }

    #[tokio::test]
    async fn given_different_text_when_embedded_then_should_differ() {
        let embedder = DeterministicEmbedder;
        let left = embedder.embed("cleared modest order").await.expect("embed");
        let right = embedder
            .embed("rejected high value ring")
            .await
            .expect("embed");
        assert_ne!(left, right);
    }

    #[test]
    fn given_a_query_when_scoring_text_then_should_prefer_overlapping_terms() {
        assert!(
            overlap_score("damaged phone refund", "phone refund approved")
                > overlap_score("damaged phone refund", "where is the parcel")
        );
    }
}
