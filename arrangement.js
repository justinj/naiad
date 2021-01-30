const { tsLess, Timestamp, MaxTimestamp } = require("./reachability");

function Arrange(def) {
  let spine = [];
  let notified = null;

  let mergePair = function* (l, r) {
    let lNext = l.next();
    let rNext = r.next();
    while (true) {
      if (lNext.done && rNext.done) {
        break;
      }
      if (lNext.done) {
        yield rNext.value;
        yield* r;
        break;
      }
      if (rNext.done) {
        yield lNext.value;
        yield* l;
        break;
      }

      let cmp = def.keyCmp(def.key(lNext.value[0]), def.key(rNext.value[0]));
      if (cmp < 0) {
        yield lNext.value;
        lNext = l.next();
      } else if (cmp > 0) {
        yield rNext.value;
        rNext = r.next();
      } else {
        // TODO: also check the value for this whole deal
        if (tsLess(lNext.value[1], rNext.value[1])) {
          yield lNext.value;
          lNext = l.next();
        } else if (tsLess(rNext.value[1], lNext.value[1])) {
          yield rNext.value;
          rNext = r.next();
        } else {
          let diff = def.diffAdd(lNext.value[2], rNext.value[2]);
          if (!def.diffIsZero(diff)) {
            yield [lNext.value[0], lNext.value[1], diff];
          }
          lNext = l.next();
          rNext = r.next();
        }
      }
    }
  };

  let merge = (i) => {
    let merged = [
      ...mergePair(
        spine[i - 1][Symbol.iterator](),
        spine[i][Symbol.iterator]()
      ),
    ];
    spine.splice(i - 1, 2, merged);
  };

  let maybeMerge = (i) => {
    if (i === 0) return;
    if (spine[i - 1].length < spine[i].length * 2) {
      merge(i);
      maybeMerge(i - 1);
    }
  };

  let self = {
    insert(data, time, diff) {
      spine.push([[data, time, diff]]);
      maybeMerge(spine.length - 1);
    },
    notify(ts) {
      if (notified === null || tsLess(notified, ts)) {
        notified = ts;
      }
    },
    lookup(ts, key) {
      // TODO: make this actually good lol
      let out = [];
      for (let row of self.read(ts)) {
        if (def.keyCmp(key, def.key(row[0])) === 0) {
          out.push(row);
        }
      }
      return out;
    },
    read(ts) {
      let out = [];
      let iter = spine[0][Symbol.iterator]();
      for (let i = 1; i < spine.length; i++) {
        iter = mergePair(iter, spine[i][Symbol.iterator]());
      }
      let curData = null;
      let curDiff = null;
      let hasData = false;
      for (let [data, time, diff] of iter) {
        if (tsLess(ts, time)) {
          continue;
        }
        if (!hasData) {
          curData = data;
          curDiff = diff;
          hasData = true;
          continue;
        }
        // TODO: also do value
        if (def.keyCmp(def.key(curData), def.key(data)) === 0) {
          curDiff = def.diffAdd(curDiff, diff);
        } else {
          if (!def.diffIsZero(curDiff)) {
            out.push([curData, curDiff]);
          }
          curData = data;
          curDiff = diff;
        }
      }
      if (hasData) {
        if (!def.diffIsZero(curDiff)) {
          out.push([curData, curDiff]);
        }
      }
      return out;
    },
  };

  return self;
}

module.exports = { Arrange };
