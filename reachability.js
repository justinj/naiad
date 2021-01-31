const { countReset } = require("console");

const Unreachable = null;

let Summary = (e, f, push) => ({ e, f, push });
let parseSummary = (s) => {
  if (s === "1") {
    return Summary(0, 0, []);
  }
  if (s === "X") {
    return Unreachable;
  }
  let e = 0;
  let i = 0;
  while (i < s.length && s[i] === "e") {
    e++;
    i++;
  }
  let f = 0;
  while (i < s.length && s[i] === "f") {
    f++;
    i++;
  }
  let push = [];
  let next = 0;
  i++;
  while (i <= s.length) {
    while (i < s.length && s[i] === "f") {
      next++;
      i++;
    }
    push.push(next);
    next = 0;
    i++;
  }
  return { e, push, f };
};
let writeSummary = (sum) => {
  if (sum === null) return "X";
  let { e, push, f } = sum;
  let out = "";
  for (let i = 0; i < e; i++) {
    out += "e";
  }
  for (let i = 0; i < f; i++) {
    out += "f";
  }
  for (let p of push) {
    out += "i";
    for (let i = 0; i < p; i++) {
      out += "f";
    }
  }
  if (out === "") {
    return "1";
  }
  return out;
};
let E = Summary(1, 0, []);
let I = Summary(0, 0, [0]);
let F = Summary(0, 1, []);
let summaryPlus = (l, r) => {
  if (l === Unreachable || r === Unreachable) {
    return Unreachable;
  }
  let result = {
    e: l.e,
    f: l.f,
    push: l.push.slice(),
  };
  for (let i = 0; i < r.e; i++) {
    if (result.push.length > 0) {
      result.push.pop();
    } else {
      result.f = 0;
      result.e++;
    }
  }
  result.f += r.f;
  result.push.push(...r.push);

  return result;
};
const less = (a, b) => {
  if (a === Unreachable) {
    return false;
  }
  if (b === Unreachable) {
    return true;
  }
  if (a.e < b.e) {
    return true;
  } else if (a.e > b.e) {
    return false;
  }
  if (a.f < b.f) {
    return true;
  } else if (a.f > b.e) {
    return false;
  }
  // By structural properties, lengths are the same.
  for (let i = 0; i < a.push.length && i < b.push.length; i++) {
    if (a.push[i] < b.push[i]) {
      return true;
    } else if (a.push[i] > b.push[i]) {
      return false;
    }
  }
  return false;
};

const Timestamp = (x) => [x, []];
const MaxTimestamp = Timestamp(Infinity);

const tsLess = ([a, as], [b, bs]) => {
  if (a < b) return true;
  if (a > b) return false;
  for (let i = 0; i < as.length || i < bs.length; i++) {
    if (as[i] < bs[i]) return true;
    if (as[i] > bs[i]) return false;
  }
  return false;
};

const tsMin = (f, ...xs) => {
  for (let x of xs) {
    if (tsLess(x, f)) f = x;
  }
  return f;
};

const tsMax = (f, ...xs) => {
  for (let x of xs) {
    if (tsLess(f, x)) f = x;
  }
  return f;
};

const apply = ([t, counters], sum) => {
  if (sum === Unreachable || t === Infinity) {
    return MaxTimestamp;
  }
  let e = sum.e;
  let f = sum.f;
  let push = sum.push;
  counters = counters.slice();
  for (let i = 0; i < e; i++) {
    counters.pop();
  }
  if (f > 0) {
    counters[counters.length - 1] += f;
  }
  counters.push(...push);
  return [t, counters];
};

module.exports = {
  Summary,
  parseSummary,
  writeSummary,
  summaryPlus,
  less,
  E,
  I,
  F,
  Timestamp,
  MaxTimestamp,
  apply,
  less,
  Unreachable,
  tsLess,
  tsMin,
  tsMax,
};
