package optimization

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/joaqu1m/sqs-batching-manager/libs/internal_errors"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

// toSQSItems wraps test-built []map[string]any into []SQSItem, reusing the map's "id" key as the SQSItem.MessageID.
// Tests still build their fixtures as maps; the conversion happens at the PackIntoSQSBatches call boundary.
func toSQSItems(items []map[string]any) []SQSItem {
	out := make([]SQSItem, len(items))
	for i, item := range items {
		id := ""
		if raw, ok := item["id"]; ok {
			id = fmt.Sprintf("%v", raw)
		}
		out[i] = SQSItem{MessageID: id, Content: item}
	}
	return out
}

// sizeAlone returns the marshaled byte size of a single item packed alone.
func sizeAlone(item map[string]any) uint64 {
	b, _ := MarshalMessageBody([]map[string]any{item})
	return uint64(len(b))
}

// sizeTogether returns the marshaled byte size of several items packed together.
func sizeTogether(items ...map[string]any) uint64 {
	b, _ := MarshalMessageBody(items)
	return uint64(len(b))
}

// decodeBatch unpacks a compressed batch back into a slice of maps.
func decodeBatch(t *testing.T, batch []byte) []map[string]any {
	t.Helper()
	var result []map[string]any
	if err := UnmarshalMessageBody(batch, &result); err != nil {
		t.Fatalf("decodeBatch failed: %v", err)
	}
	return result
}

// countPackedItems counts total items across all decoded batches.
func countPackedItems(t *testing.T, packed []PackedBatch) int {
	t.Helper()
	n := 0
	for _, b := range packed {
		n += len(decodeBatch(t, b.Bytes))
	}
	return n
}

// findBatch returns the index of the first batch that contains an item with id == target.
func findBatch(t *testing.T, packed []PackedBatch, targetID string) (int, bool) {
	t.Helper()
	for i, b := range packed {
		for _, item := range decodeBatch(t, b.Bytes) {
			if fmt.Sprintf("%v", item["id"]) == targetID {
				return i, true
			}
		}
	}
	return -1, false
}

// ceilingFor mirrors the algorithm's threshold selection for a given byte size.
func ceilingFor(value uint64, thresholds []uint64) uint64 {
	sorted := make([]uint64, len(thresholds))
	copy(sorted, thresholds)
	slices.Sort(sorted)
	for _, t := range sorted {
		if value <= t {
			return t
		}
	}
	return sorted[len(sorted)-1]
}

// newItem creates a minimal test item.
func newItem(id string) map[string]any {
	return map[string]any{"id": id}
}

// newPaddedItem creates an item with high-entropy padding that resists
// compression, giving predictable serialized sizes.
func newPaddedItem(id string, padLen int) map[string]any {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	pad := make([]byte, padLen)
	// FNV-1a-inspired mixing: mixes position, padLen and seed to avoid
	// patterns that zstd/LZ would detect and collapse.
	h := uint64(14695981039346656037)
	for i := range pad {
		h ^= uint64(i*padLen + 97)
		h *= 1099511628211
		pad[i] = chars[h%uint64(len(chars))]
	}
	return map[string]any{"id": id, "data": string(pad)}
}

// ─── 1. Single threshold ──────────────────────────────────────────────────────

func TestSingleThreshold_AllItemsFitOneBatch(t *testing.T) {
	items := []map[string]any{
		newItem("a"), newItem("b"), newItem("c"), newItem("d"), newItem("e"),
	}
	threshold := sizeTogether(items...) + 100

	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{threshold})

	if len(errored) != 0 {
		t.Fatalf("expected 0 errored, got %d", len(errored))
	}
	if len(packed) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(packed))
	}
	if countPackedItems(t, packed) != 5 {
		t.Errorf("expected 5 items in batch, got %d", countPackedItems(t, packed))
	}
}

func TestSingleThreshold_SplitAcrossMultipleBatches(t *testing.T) {
	items := []map[string]any{
		newPaddedItem("a", 80), newPaddedItem("b", 80), newPaddedItem("c", 80),
		newPaddedItem("d", 80), newPaddedItem("e", 80),
	}
	// Threshold = size of one item alone.
	// With a warm high-compression encoder, similar items may compress better
	// together than alone, so the exact batch count is not a stable invariant.
	// We verify the correct invariants: no items lost, no batch exceeds threshold.
	threshold := sizeAlone(items[0])

	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{threshold})

	if len(errored) != 0 {
		t.Fatalf("expected 0 errored, got %d", len(errored))
	}
	if countPackedItems(t, packed) != 5 {
		t.Errorf("expected 5 total items, got %d", countPackedItems(t, packed))
	}
	for i, batch := range packed {
		if uint64(len(batch.Bytes)) > threshold {
			t.Errorf("batch %d size %d exceeds threshold %d", i, len(batch.Bytes), threshold)
		}
	}
}

// ─── 2. Multiple thresholds / escalation ──────────────────────────────────────

func TestMultipleThresholds_AllFitLowestTier(t *testing.T) {
	items := []map[string]any{
		newItem("a"), newItem("b"), newItem("c"), newItem("d"), newItem("e"),
	}
	// Each item is tiny; low threshold fits them all individually.
	lowThreshold := sizeAlone(items[0]) + 50
	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{lowThreshold, lowThreshold * 2, lowThreshold * 4})

	if len(errored) != 0 {
		t.Fatalf("expected 0 errored, got %d", len(errored))
	}
	// Every batch must fit within the low threshold.
	for i, batch := range packed {
		if uint64(len(batch.Bytes)) > lowThreshold {
			t.Errorf("batch %d size %d exceeds low threshold %d", i, len(batch.Bytes), lowThreshold)
		}
	}
}

func TestMultipleThresholds_LeaderEscalatesToSecondTier(t *testing.T) {
	small := newPaddedItem("small", 10)
	large := newPaddedItem("large", 200)

	smallSize := sizeAlone(small)
	largeSize := sizeAlone(large)

	lowThreshold := smallSize + 30
	highThreshold := largeSize + 200

	if largeSize <= lowThreshold {
		t.Skip("large item fits in low threshold; adjust padding sizes")
	}

	items := []map[string]any{large, small}
	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{lowThreshold, highThreshold})

	if len(errored) != 0 {
		t.Fatalf("expected 0 errored, got %d: %v", len(errored), errored[0].Err)
	}

	largeBatchIdx, found := findBatch(t, packed, "large")
	if !found {
		t.Fatal("item 'large' not found in any batch")
	}

	// Batch with large must not exceed highThreshold.
	if uint64(len(packed[largeBatchIdx].Bytes)) > highThreshold {
		t.Errorf("large batch size %d exceeds highThreshold %d", len(packed[largeBatchIdx].Bytes), highThreshold)
	}
	// Batch with large must exceed lowThreshold (large alone already does).
	if uint64(len(packed[largeBatchIdx].Bytes)) <= lowThreshold {
		t.Errorf("expected large batch to exceed lowThreshold %d, got %d", lowThreshold, len(packed[largeBatchIdx].Bytes))
	}
}

func TestMultipleThresholds_LeaderEscalatesToMaxTier(t *testing.T) {
	huge := newPaddedItem("huge", 500)
	hugeSize := sizeAlone(huge)

	lowThreshold := hugeSize / 3
	midThreshold := hugeSize / 2
	maxThreshold := hugeSize + 300

	items := []map[string]any{
		huge,
		newPaddedItem("s1", 10),
		newPaddedItem("s2", 10),
		newPaddedItem("s3", 10),
	}
	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{lowThreshold, midThreshold, maxThreshold})

	if len(errored) != 0 {
		t.Fatalf("expected 0 errored, got %d", len(errored))
	}

	hugeBatchIdx, found := findBatch(t, packed, "huge")
	if !found {
		t.Fatal("item 'huge' not found")
	}
	if uint64(len(packed[hugeBatchIdx].Bytes)) > maxThreshold {
		t.Errorf("huge batch size %d exceeds max threshold %d", len(packed[hugeBatchIdx].Bytes), maxThreshold)
	}
}

func TestMultipleThresholds_MixedLeadersRespectCeilings(t *testing.T) {
	items := []map[string]any{
		newPaddedItem("xl", 400),
		newPaddedItem("lg", 200),
		newPaddedItem("md1", 80),
		newPaddedItem("md2", 70),
		newPaddedItem("sm1", 10),
		newPaddedItem("sm2", 10),
		newPaddedItem("sm3", 10),
		newPaddedItem("sm4", 10),
	}

	xlSize := sizeAlone(items[0])
	lgSize := sizeAlone(items[1])
	mdSize := sizeAlone(items[2])

	t1 := mdSize + 50
	t2 := lgSize + 100
	t3 := xlSize + 300

	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{t1, t2, t3})

	if len(errored) != 0 {
		t.Fatalf("expected 0 errored, got %d", len(errored))
	}
	if countPackedItems(t, packed) != len(items) {
		t.Fatalf("expected %d packed items, got %d", len(items), countPackedItems(t, packed))
	}

	// Every batch must not exceed the ceiling determined by its leader.
	for i, batch := range packed {
		decoded := decodeBatch(t, batch.Bytes)
		leaderSize := sizeAlone(decoded[0])
		ceiling := ceilingFor(leaderSize, []uint64{t1, t2, t3})
		batchSize := uint64(len(batch.Bytes))
		if batchSize > ceiling {
			t.Errorf("batch %d: size %d > ceiling %d (leader '%v', size %d)",
				i, batchSize, ceiling, decoded[0]["id"], leaderSize)
		}
	}
}

// ─── 3. Filling / greedy packing ─────────────────────────────────────────────

func TestFilling_SmallItemsFillLeftoverSpace(t *testing.T) {
	leader := newPaddedItem("leader", 150)
	fillers := []map[string]any{
		newPaddedItem("f1", 20),
		newPaddedItem("f2", 20),
		newPaddedItem("f3", 20),
		newPaddedItem("f4", 20),
	}

	// Threshold: fits leader alone + at least one filler.
	leaderWithOneFiller := sizeTogether(leader, fillers[0])
	threshold := leaderWithOneFiller + 10

	items := append([]map[string]any{leader}, fillers...)
	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{threshold})

	if len(errored) != 0 {
		t.Fatalf("expected 0 errored, got %d", len(errored))
	}

	leaderBatchIdx, found := findBatch(t, packed, "leader")
	if !found {
		t.Fatal("leader not found in any batch")
	}

	leaderBatch := decodeBatch(t, packed[leaderBatchIdx].Bytes)
	if len(leaderBatch) < 2 {
		t.Errorf("expected leader batch to contain at least 2 items (leader + 1 filler), got %d", len(leaderBatch))
	}
	if uint64(len(packed[leaderBatchIdx].Bytes)) > threshold {
		t.Errorf("leader batch size %d exceeds threshold %d", len(packed[leaderBatchIdx].Bytes), threshold)
	}
}

func TestFilling_NoLeftoverFits(t *testing.T) {
	leader := newPaddedItem("leader", 200)
	others := []map[string]any{
		newPaddedItem("a", 200),
		newPaddedItem("b", 200),
		newPaddedItem("c", 200),
	}

	// Threshold: just barely fits leader alone.
	threshold := sizeAlone(leader)

	items := append([]map[string]any{leader}, others...)
	packed, _ := PackIntoSQSBatches(toSQSItems(items),[]uint64{threshold})

	leaderBatchIdx, found := findBatch(t, packed, "leader")
	if !found {
		t.Fatal("leader not found")
	}

	leaderBatch := decodeBatch(t, packed[leaderBatchIdx].Bytes)
	if len(leaderBatch) != 1 {
		t.Errorf("expected leader batch to have exactly 1 item (no room for fillers), got %d", len(leaderBatch))
	}
}

// ─── 4. Errored items ─────────────────────────────────────────────────────────

func TestErrored_OneItemExceedsMax(t *testing.T) {
	normal := newPaddedItem("ok", 10)
	normalSize := sizeAlone(normal)
	huge := newPaddedItem("over", 2000)
	hugeSize := sizeAlone(huge)

	maxThreshold := normalSize + 50
	if hugeSize <= maxThreshold {
		t.Skip("huge item fits; adjust padding sizes")
	}

	items := []map[string]any{huge, normal}
	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{maxThreshold})

	if len(errored) != 1 {
		t.Fatalf("expected 1 errored item, got %d", len(errored))
	}
	if errored[0].MessageID != "over" {
		t.Errorf("expected errored item 'over', got %v", errored[0].MessageID)
	}
	if countPackedItems(t, packed) != 1 {
		t.Fatalf("expected 1 packed item, got %d", countPackedItems(t, packed))
	}

	var sizeErr internal_errors.ExceededAllowedSizeError
	if !errors.As(errored[0].Err, &sizeErr) {
		t.Errorf("expected ExceededAllowedSizeError, got %T: %v", errored[0].Err, errored[0].Err)
	}
}

func TestErrored_MultipleItemsExceedMax(t *testing.T) {
	normal := newPaddedItem("ref", 10)
	maxThreshold := sizeAlone(normal) + 50

	items := []map[string]any{
		newPaddedItem("ok1", 10),
		newPaddedItem("over1", 2000),
		newPaddedItem("ok2", 10),
		newPaddedItem("over2", 2000),
		newPaddedItem("ok3", 10),
		newPaddedItem("over3", 2000),
	}

	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{maxThreshold})

	if len(errored) != 3 {
		t.Fatalf("expected 3 errored items, got %d", len(errored))
	}
	if countPackedItems(t, packed) != 3 {
		t.Errorf("expected 3 packed items, got %d", countPackedItems(t, packed))
	}

	erroredIDs := map[string]bool{}
	for _, e := range errored {
		erroredIDs[e.MessageID] = true
	}
	for _, expected := range []string{"over1", "over2", "over3"} {
		if !erroredIDs[expected] {
			t.Errorf("expected '%s' in errored items", expected)
		}
	}
}

func TestErrored_AllItemsExceedMax(t *testing.T) {
	items := []map[string]any{
		newPaddedItem("a", 500),
		newPaddedItem("b", 500),
		newPaddedItem("c", 500),
	}
	// Threshold too small for any item.
	threshold := uint64(5)

	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{threshold})

	if len(errored) != 3 {
		t.Fatalf("expected 3 errored items, got %d", len(errored))
	}
	if len(packed) != 0 {
		t.Errorf("expected 0 batches, got %d", len(packed))
	}
}

// ─── 5. Invariants ────────────────────────────────────────────────────────────

func TestInvariant_NoItemsLost(t *testing.T) {
	normal := newPaddedItem("ref", 10)
	maxThreshold := sizeAlone(normal) + 50

	items := []map[string]any{
		newPaddedItem("ok1", 10),
		newPaddedItem("ok2", 10),
		newPaddedItem("err1", 2000),
		newPaddedItem("ok3", 10),
		newPaddedItem("err2", 2000),
	}

	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{maxThreshold})

	total := countPackedItems(t, packed) + len(errored)
	if total != len(items) {
		t.Errorf("expected %d total (packed+errored), got %d", len(items), total)
	}
	if len(errored) != 2 {
		t.Errorf("expected 2 errored, got %d", len(errored))
	}
}

func TestInvariant_AllBatchesBelowThreshold(t *testing.T) {
	items := make([]map[string]any, 15)
	for i := range items {
		items[i] = newPaddedItem(fmt.Sprintf("item-%d", i), 20+i*5)
	}

	var maxSize uint64
	for _, item := range items {
		if s := sizeAlone(item); s > maxSize {
			maxSize = s
		}
	}

	thresholds := []uint64{maxSize, maxSize * 2, maxSize * 4}
	packed, errored := PackIntoSQSBatches(toSQSItems(items),thresholds)

	if len(errored) != 0 {
		t.Fatalf("expected 0 errored, got %d", len(errored))
	}

	maxThreshold := thresholds[len(thresholds)-1]
	for i, batch := range packed {
		if uint64(len(batch.Bytes)) > maxThreshold {
			t.Errorf("batch %d size %d exceeds max threshold %d", i, len(batch.Bytes), maxThreshold)
		}
	}
	if countPackedItems(t, packed) != len(items) {
		t.Errorf("expected %d total items, got %d", len(items), countPackedItems(t, packed))
	}
}

// ─── 6. FFD ordering ──────────────────────────────────────────────────────────

func TestOrdering_LeaderIsLargestInBatch(t *testing.T) {
	items := []map[string]any{
		newPaddedItem("small1", 10),
		newPaddedItem("large1", 200),
		newPaddedItem("small2", 10),
		newPaddedItem("large2", 200),
		newPaddedItem("medium", 80),
	}

	largeSize := sizeAlone(newPaddedItem("ref", 200))
	threshold := largeSize + 300

	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{threshold})

	if len(errored) != 0 {
		t.Fatalf("expected 0 errored, got %d", len(errored))
	}

	for i, batch := range packed {
		decoded := decodeBatch(t, batch.Bytes)
		if len(decoded) == 0 {
			t.Errorf("batch %d is empty", i)
			continue
		}
		leaderSize := sizeAlone(decoded[0])
		for j := 1; j < len(decoded); j++ {
			if s := sizeAlone(decoded[j]); s > leaderSize {
				t.Errorf("batch %d: item %d (id=%v, size=%d) > leader (size=%d)",
					i, j, decoded[j]["id"], s, leaderSize)
			}
		}
	}
}

func TestOrdering_InputOrderDoesNotAffectResult(t *testing.T) {
	// Ascending vs descending input should produce identical packing structure.
	// We compare decoded item counts and leaders — not raw byte sizes, because
	// Go map iteration is non-deterministic and msgpack may serialize keys in
	// different orders across runs.
	ascending := []map[string]any{
		newPaddedItem("sm", 10),
		newPaddedItem("md", 80),
		newPaddedItem("lg", 200),
	}
	descending := []map[string]any{
		newPaddedItem("lg", 200),
		newPaddedItem("md", 80),
		newPaddedItem("sm", 10),
	}

	lgSize := sizeAlone(newPaddedItem("lg", 200))
	threshold := lgSize + 500

	packedAsc, _ := PackIntoSQSBatches(toSQSItems(ascending), []uint64{threshold})
	packedDesc, _ := PackIntoSQSBatches(toSQSItems(descending), []uint64{threshold})

	if len(packedAsc) != len(packedDesc) {
		t.Fatalf("different batch counts: asc=%d desc=%d", len(packedAsc), len(packedDesc))
	}
	// Each corresponding batch must have the same number of items and the same leader id.
	for i := range packedAsc {
		decAsc := decodeBatch(t, packedAsc[i].Bytes)
		decDesc := decodeBatch(t, packedDesc[i].Bytes)
		if len(decAsc) != len(decDesc) {
			t.Errorf("batch %d: asc items %d != desc items %d", i, len(decAsc), len(decDesc))
			continue
		}
		leaderAsc := fmt.Sprintf("%v", decAsc[0]["id"])
		leaderDesc := fmt.Sprintf("%v", decDesc[0]["id"])
		if leaderAsc != leaderDesc {
			t.Errorf("batch %d: different leaders: asc=%s desc=%s", i, leaderAsc, leaderDesc)
		}
	}
}

// ─── 7. Threshold order independence ─────────────────────────────────────────

func TestThresholdOrder_UnorderedProducesSameResult(t *testing.T) {
	makeItems := func() []map[string]any {
		return []map[string]any{
			newPaddedItem("a", 300), newPaddedItem("b", 150), newPaddedItem("c", 80),
			newPaddedItem("d", 40), newPaddedItem("e", 10), newPaddedItem("f", 10),
		}
	}

	maxSize := sizeAlone(newPaddedItem("ref", 300))
	t1 := maxSize / 3
	t2 := maxSize
	t3 := maxSize * 3

	pack := func(items []map[string]any, thresholds []uint64) []PackedBatch {
		p, _ := PackIntoSQSBatches(toSQSItems(items),thresholds)
		return p
	}

	p1 := pack(makeItems(), []uint64{t1, t2, t3})
	p2 := pack(makeItems(), []uint64{t3, t1, t2})
	p3 := pack(makeItems(), []uint64{t2, t3, t1})

	if len(p1) != len(p2) || len(p2) != len(p3) {
		t.Fatalf("batch counts differ: %d, %d, %d", len(p1), len(p2), len(p3))
	}
	// Compare decoded item counts per batch, not byte sizes.
	// A warm high-compression encoder may produce slightly different byte sizes
	// for identical content depending on encoder state, but the grouping must be identical.
	for i := range p1 {
		n1 := len(decodeBatch(t, p1[i].Bytes))
		n2 := len(decodeBatch(t, p2[i].Bytes))
		n3 := len(decodeBatch(t, p3[i].Bytes))
		if n1 != n2 || n2 != n3 {
			t.Errorf("batch %d item counts differ: %d, %d, %d", i, n1, n2, n3)
		}
	}
}

// ─── 8. Content integrity ─────────────────────────────────────────────────────

func TestContentIntegrity_ItemsSurviveRoundTrip(t *testing.T) {
	items := []map[string]any{
		{"id": "alice", "city": "SP"},
		{"id": "bob", "city": "RJ"},
		{"id": "charlie", "city": "BH"},
	}
	threshold := sizeTogether(items...) + 100

	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{threshold})

	if len(errored) != 0 {
		t.Fatalf("expected 0 errored, got %d", len(errored))
	}

	found := map[string]bool{"alice": false, "bob": false, "charlie": false}
	for _, batch := range packed {
		for _, item := range decodeBatch(t, batch.Bytes) {
			id := fmt.Sprintf("%v", item["id"])
			if _, exists := found[id]; exists {
				found[id] = true
			} else {
				t.Errorf("unexpected item id: %s", id)
			}
		}
	}
	for id, ok := range found {
		if !ok {
			t.Errorf("item '%s' not found after round-trip", id)
		}
	}
}

func TestContentIntegrity_NoDuplicates(t *testing.T) {
	items := make([]map[string]any, 10)
	for i := range items {
		items[i] = newPaddedItem(fmt.Sprintf("item-%d", i), 40)
	}

	maxSize := sizeAlone(items[0])
	packed, errored := PackIntoSQSBatches(toSQSItems(items),[]uint64{maxSize, maxSize * 2, maxSize * 4})

	if len(errored) != 0 {
		t.Fatalf("expected 0 errored, got %d", len(errored))
	}

	seen := map[string]int{}
	for i, batch := range packed {
		for _, item := range decodeBatch(t, batch.Bytes) {
			id := fmt.Sprintf("%v", item["id"])
			if prev, ok := seen[id]; ok {
				t.Errorf("item '%s' appears in batch %d and batch %d", id, prev, i)
			}
			seen[id] = i
		}
	}
	if len(seen) != len(items) {
		t.Errorf("expected %d unique items, got %d", len(items), len(seen))
	}
}

// ─── 9. Scale ─────────────────────────────────────────────────────────────────

func TestScale_ManyItemsAllPackedAndRespectThresholds(t *testing.T) {
	items := make([]map[string]any, 50)
	for i := range items {
		items[i] = newPaddedItem(fmt.Sprintf("item-%d", i), 10+i%5*20)
	}

	var maxSize uint64
	for _, item := range items {
		if s := sizeAlone(item); s > maxSize {
			maxSize = s
		}
	}

	thresholds := []uint64{maxSize, maxSize * 2, maxSize * 4}
	packed, errored := PackIntoSQSBatches(toSQSItems(items),thresholds)

	if len(errored) != 0 {
		t.Fatalf("expected 0 errored, got %d", len(errored))
	}
	if countPackedItems(t, packed) != len(items) {
		t.Fatalf("expected %d items packed, got %d", len(items), countPackedItems(t, packed))
	}

	maxThreshold := thresholds[len(thresholds)-1]
	for i, batch := range packed {
		decoded := decodeBatch(t, batch.Bytes)
		if uint64(len(batch.Bytes)) > maxThreshold {
			t.Errorf("batch %d size %d > max threshold %d", i, len(batch.Bytes), maxThreshold)
		}
		// Leader must be the largest item in the batch by sizeAlone.
		if len(decoded) > 1 {
			leaderSize := sizeAlone(decoded[0])
			for j := 1; j < len(decoded); j++ {
				if s := sizeAlone(decoded[j]); s > leaderSize {
					t.Errorf("batch %d: item %d larger than leader", i, j)
				}
			}
		}
	}

	// No item appears more than once.
	seen := map[string]bool{}
	for _, batch := range packed {
		for _, item := range decodeBatch(t, batch.Bytes) {
			id := fmt.Sprintf("%v", item["id"])
			if seen[id] {
				t.Errorf("duplicate item: %s", id)
			}
			seen[id] = true
		}
	}
}
