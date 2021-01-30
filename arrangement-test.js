const assert = require("assert");
const { Arrange } = require("./arrangement");
const { Timestamp } = require("./reachability");

let arr = () =>
  Arrange({
    key(row) {
      return row;
    },
    value(row) {
      return null;
    },
    keyCmp(k1, k2) {
      return k1 < k2 ? -1 : k1 === k2 ? 0 : 1;
    },
    valueCmp(v1, v2) {
      return 0;
    },
    diffAdd(a, b) {
      return a + b;
    },
    diffIsZero(a) {
      return a === 0;
    },
  });

describe("arrangement", () => {
  it("lets you insert records", () => {
    let a = arr();

    a.insert(1, Timestamp(0), 1);
    a.insert(2, Timestamp(0), 1);
    a.insert(3, Timestamp(0), 1);
    a.notify(Timestamp(1));

    assert.deepStrictEqual(
      [...a.read(Timestamp(0))].map((t) => t[0]),
      [1, 2, 3]
    );
  });

  it("lets you insert records", () => {
    let a = arr();

    a.insert(1, Timestamp(0), 1);
    a.insert(2, Timestamp(0), 1);
    a.insert(2, Timestamp(0), -1);
    a.insert(3, Timestamp(0), 1);
    a.notify(Timestamp(1));

    assert.deepStrictEqual(
      [...a.read(Timestamp(0))].map((t) => t[0]),
      [1, 3]
    );
  });

  it("handles timestamps", () => {
    let a = arr();

    a.insert(1, Timestamp(0), 1);
    a.insert(1, Timestamp(1), -1);
    a.notify(Timestamp(2));

    assert.deepStrictEqual(
      [...a.read(Timestamp(0))].map((t) => t[0]),
      [1]
    );
    assert.deepStrictEqual(
      [...a.read(Timestamp(1))].map((t) => t[0]),
      []
    );
  });

  it("handles lookups", () => {
    let a = arr();

    a.insert(1, Timestamp(0), 1);
    a.insert(2, Timestamp(0), 1);
    a.insert(1, Timestamp(1), -1);
    a.insert(3, Timestamp(1), -1);
    a.notify(Timestamp(2));

    assert.deepStrictEqual(
      [...a.lookup(Timestamp(0), 1)].map((t) => t[0]),
      [1]
    );
    assert.deepStrictEqual(
      [...a.lookup(Timestamp(1), 1)].map((t) => t[0]),
      []
    );
  });
});
