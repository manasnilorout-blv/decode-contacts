const ContactDeduplicator = require('./dedupe');

async function testSimilarity() {
  const deduplicator = new ContactDeduplicator();
  
  const name1 = "abhijit hazari";
  const name2 = "cg abhijit hazari";
  
  const normalized1 = deduplicator.normalizeName(name1);
  const normalized2 = deduplicator.normalizeName(name2);
  
  console.log(`Name 1: "${name1}" -> "${normalized1}"`);
  console.log(`Name 2: "${name2}" -> "${normalized2}"`);
  
  const similar = deduplicator.areNamesSimilar(normalized1, normalized2, 0.8);
  console.log(`Are similar (threshold 0.8): ${similar}`);
  
  const similar2 = deduplicator.areNamesSimilar(normalized1, normalized2, 0.7);
  console.log(`Are similar (threshold 0.7): ${similar2}`);
  
  const similar3 = deduplicator.areNamesSimilar(normalized1, normalized2, 0.6);
  console.log(`Are similar (threshold 0.6): ${similar3}`);
}

testSimilarity();