const assert = require("assert");
const {
  Summary,
  parseSummary,
  writeSummary,
  summaryPlus,
} = require("./reachability");

describe("summaries", () => {
  it("parses and renders", () => {
    for (let [str, expected] of [
      ["iii", Summary(0, 0, [0, 0, 0])],
      ["e", Summary(1, 0, [])],
      ["i", Summary(0, 0, [0])],
      ["eeeiii", Summary(3, 0, [0, 0, 0])],
      ["fff", Summary(0, 3, [])],
      ["fiffifffiff", Summary(0, 1, [2, 3, 2])],
    ]) {
      let parsed = parseSummary(str);
      assert.deepStrictEqual(parsed, expected);
      let written = writeSummary(parsed);
      assert.strictEqual(written, str);
    }
  });

  it("adds two summaries", () => {
    for (let [a, b, expected] of [
      ["e", "i", "ei"],
      ["i", "e", "1"],
      ["e", "ei", "eei"],
      ["ef", "ei", "eei"],
      ["eiiif", "ei", "eiii"],
    ]) {
      a = parseSummary(a);
      b = parseSummary(b);
      let actual = summaryPlus(a, b);
      expected = parseSummary(expected);
      assert.deepStrictEqual(actual, expected);
    }
  });
});
