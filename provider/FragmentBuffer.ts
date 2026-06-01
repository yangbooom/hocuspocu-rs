export default class FragmentBuffer {
  id: string;

  chunks: Map<number, Uint8Array>; // Map of chunk index to data

  expectedChunks: number | null; // Total chunks expected, if known

  receivedEnd: boolean;

  constructor(uniqueFragmentId: string) {
    this.id = uniqueFragmentId;
    this.chunks = new Map(); // Stores chunks by index
    this.expectedChunks = null; // If metadata provides total
    this.receivedEnd = false;
  }

  addChunk(index: number, data: Uint8Array) {
    this.chunks.set(index, data);
  }

  // Optional: Call this if FragmentStart provides total chunks
  setExpectedChunks(count: number) {
    this.expectedChunks = count;
  }

  markEndReceived() {
    this.receivedEnd = true;
  }

  isComplete() {
    // Check if all chunks received and end marker
    // If expectedChunks is known:
    // return this.receivedEnd && this.chunks.size === this.expectedChunks;
    // Otherwise, more complex logic (e.g., looking for sequential gaps)
    return this.receivedEnd && this._hasAllSequentialChunks();
  }

  _hasAllSequentialChunks() {
    if (this.chunks.size === 0) return false;
    let maxIndex = -1;
    // Convert keys to array and find the maximum index
    const keys = Array.from(this.chunks.keys());
    keys.forEach((key: number) => {
      if (key > maxIndex) maxIndex = key;
    });
    return this.chunks.size === maxIndex + 1; // Checks for contiguity
  }

  getCombinedBytes() {
    if (!this.isComplete()) {
      throw new Error('Cannot combine incomplete fragment!');
    }
    const sortedChunks = Array.from(this.chunks.entries())
      .sort(([idxA], [idxB]) => idxA - idxB)
      .map(([, data]) => data);

    // Calculate total size
    const totalLength = sortedChunks.reduce((sum, chunk) => sum + chunk.length, 0);

    // Concatenate all chunks
    const combined = new Uint8Array(totalLength);
    let offset = 0;
    sortedChunks.forEach(chunk => {
      combined.set(chunk, offset);
      offset += chunk.length;
    });
    return combined;
  }
}

export const activeFragmentTransmissions: Map<string, FragmentBuffer> = new Map();
