const { DataflowBuilder } = require("./push");
const {
  tsMin,
  tsMax,
  tsLess,
  I,
  E,
  F,
  apply,
  Timestamp,
  Summary,
  summaryPlus,
} = require("./reachability");
const Query = () => {
  let builder = DataflowBuilder();

  const asStream = (node, summary = Summary(0, 0, [])) => {
    let makeScope = (summary) => {
      let t = apply(Timestamp(0), summary);
      let scope = {
        node,
        asStream(node) {
          return asStream(node, summary);
        },
        forEach(f) {
          let next = scope.vertex((send, notify) => ({
            recv(e, m, t) {
              f(send, e, m, t);
            },
            onNotify(t) {
              notify(t);
            },
          }));
          builder.edge(node, 0, next, 0);

          return scope.asStream(next);
        },
        enter({ node }) {
          let next = scope.vertex((send, notify) => ({
            recv(e, m, t) {
              send(e, m, apply(t, summary));
            },
            onNotify(t) {
              notify(t);
            },
          }));
          builder.edge(node, 0, next, 0, summary);

          return scope.asStream(next);
        },
        flatMap(f) {
          return scope.forEach((send, e, m, t) => {
            for (let x of f(m)) {
              send(e, x, t);
            }
          });
        },
        concat(other) {
          let next = scope.vertex((send, notify) => ({
            recv(e, m, t) {
              send(0, m, t);
            },
            onNotify(t) {
              notify(t);
            },
          }));
          builder.edge(node, 0, next, 0);
          builder.edge(other.node, 0, next, 1);

          return scope.asStream(next);
        },
        passthrough(s) {
          return scope.vertex((send, notify) => ({
            recv(e, m, t) {
              send(0, m, apply(t, s));
            },
            onNotify(t) {
              notify(apply(t, s));
            },
          }));
        },
        feedback() {
          return scope.passthrough(F);
        },
        ingress() {
          return scope.passthrough(I);
        },
        egress() {
          return scope.passthrough(E);
        },
        select(f) {
          return scope.flatMap((m) => (f(m) ? [m] : []));
        },
        map(f) {
          return scope.flatMap((m) => [f(m)]);
        },
        vertex(defn = () => ({})) {
          return builder.vertex(t, defn);
        },
        iterate(other) {
          let inner = makeScope(summaryPlus(summary, Summary(0, 0, [0])));
          let i = inner.ingress();
          let f = inner.feedback();
          let conc = inner.vertex();
          let out = inner.vertex();
          let c = inner.vertex();

          let computation = other(inner.asStream(c));

          let e = scope.egress();

          builder.edge(node, 0, i, 0);
          builder.edge(i, 0, conc, 0, I);
          builder.edge(conc, 0, c, 0);
          builder.edge(computation.node, 0, f, 0);
          builder.edge(f, 0, conc, 1, F);
          builder.edge(computation.node, 0, e, 0);
          builder.edge(e, 0, out, 0, E);

          return scope.asStream(out);
        },
        join(
          other,
          leftKey = (x) => x.toString(),
          rightKey = (x) => x.toString(),
          combine = (x, y) => [x, y]
        ) {
          let leftTable = {};
          let rightTable = {};
          let next = scope.vertex((send, notify) => ({
            recv(e, m, t) {
              if (e === 0) {
                let key = leftKey(m);
                if (!leftTable.hasOwnProperty(key)) {
                  leftTable[key] = [];
                }
                leftTable[key].push([m, t]);
                for (let [row, ts] of rightTable[key] || []) {
                  // console.log("join emitting", combine(m, row));
                  send(0, combine(m, row), tsMax(ts, t));
                }
              } else if (e === 1) {
                let key = rightKey(m);
                if (!rightTable.hasOwnProperty(key)) {
                  rightTable[key] = [];
                }
                rightTable[key].push([m, t]);
                for (let [row, ts] of leftTable[key] || []) {
                  // console.log("join emitting", combine(row, m));
                  send(0, combine(row, m), tsMax(ts, t));
                }
              }
            },
            onNotify(ts) {
              notify(ts);
            },
          }));

          builder.edge(node, 0, next, 0);
          builder.edge(other.node, 0, next, 1);

          return scope.asStream(next);
        },
        distinct(hash = (x) => "" + x) {
          let seen = {};
          let queue = [];
          let next = scope.vertex((send, notify, pending) => ({
            recv(e, m, t) {
              let key = hash(m);
              if (!seen.hasOwnProperty(key)) {
                seen[key] = { m, t };
                queue.push(seen[key]);
                pending(1);
              } else if (tsLess(t, seen[key].t)) {
                seen[key].t = t;
              }
            },
            onNotify(ts) {
              let newQueue = [];
              for (let { m, t } of queue) {
                if (tsLess(t, ts)) {
                  send(0, m, t);
                  pending(-1);
                } else {
                  newQueue.push({ m, t });
                }
              }
              queue = newQueue;
              notify(tsMin(ts, ...queue.map((x) => x.t)));
            },
          }));
          builder.edge(node, 0, next, 0);

          return scope.asStream(next);
        },
        mapBoth(f, { diffIsZero } = { diffIsZero: (a) => a === 0 }) {
          return scope.flatMap(([m, d]) => {
            let [newM, newD] = f(m, d);
            if (diffIsZero(newD)) {
              return [];
            } else {
              return [[newM, newD]];
            }
          });
        },
        diffDistinct({ diffIsZero }) {
          return scope
            .reduce(
              (x) => x,
              () => true
            )
            .mapBoth(([m, d1], d2) => {
              if (diffIsZero(d1)) {
                return [m, -Math.sign(d2)];
              } else {
                return [m, Math.sign(d2)];
              }
            })
            .consolidate({
              hash: (x) => x,
              diffAdd: (a, b) => a + b,
              diffIsZero: (a) => a === 0,
            });
        },
        reduce(key, combine) {
          let groups = {};
          return (
            scope
              .consolidate({
                hash: key,
                diffAdd: (a, b) => a + b,
                diffIsZero: (a) => a === 0,
              })
              // .inspect((x, t) => console.log(`c1 emitting ${x}@${t}`))
              .forEach((send, _, [m, d], t) => {
                // console.log("foreach seeing", m, d, t);
                let k = key(m);
                if (groups.hasOwnProperty(k)) {
                  let [oldMult, oldVal] = groups[k];
                  let added = combine(oldVal, m * d);
                  send(0, [[k, oldVal], -1], t);
                  if (oldMult + d === 0) {
                    delete groups[k];
                  } else {
                    send(0, [[k, added], 1], t);
                    groups[k] = [oldMult + d, added];
                  }
                  // console.log(groups);
                } else {
                  groups[k] = [d, m * d];
                  send(0, [[k, m * d], 1], t);
                }
              })
          );
          // .inspect((x, t) => console.log(`forEach emitting ${x}@${t}`));
        },
        consolidate({ hash, diffAdd, diffIsZero }) {
          let out = [];
          let next = scope.vertex((send, notify, pending) => ({
            recv(e, m, t) {
              pending(1);
              out.push([m, t]);
            },
            onNotify(t) {
              out.sort(([m1, t1], [m2, t2]) => (tsLess(t1, t2) ? -1 : 1));
              let i = 0;
              while (i < out.length && tsLess(out[i][1], t)) {
                let lowestTs = out[i][1];
                let epoch = [];
                while (i < out.length && !tsLess(lowestTs, out[i][1])) {
                  pending(-1);
                  epoch.push(out[i][0]);
                  i++;
                }
                let grouped = {};
                let ks = [];
                for (let [m, diff] of epoch) {
                  let k = hash(m);
                  if (grouped.hasOwnProperty(k)) {
                    grouped[k][1] = diffAdd(grouped[k][1], diff);
                  } else {
                    grouped[k] = [m, diff];
                    ks.push(k);
                  }
                }
                for (let k of ks) {
                  if (!diffIsZero(grouped[k][1])) {
                    send(0, grouped[k], lowestTs);
                  }
                }
              }

              out = out.slice(i);
              notify(t);
            },
          }));
          builder.edge(node, 0, next, 0);

          return scope.asStream(next);
        },
        fence() {
          let out = [];
          let next = scope.vertex((send, notify) => ({
            recv(e, m, t) {
              out.push([m, t]);
            },
            onNotify(t) {
              let newOut = [];
              for (let [m, ts] of out) {
                if (tsLess(ts, t)) {
                  send(0, m, ts);
                } else {
                  newOut.push([m, ts]);
                }
              }
              out = newOut;
              notify(t);
            },
          }));
          builder.edge(node, 0, next, 0);

          return scope.asStream(next);
        },
        inspect(f) {
          return scope.forEach((send, e, m, t) => {
            f(m, t);
            send(e, m, t);
          });
        },
      };
      return scope;
    };

    return {
      ...makeScope(summary),
      Build() {
        builder.build();
      },
      RunTo(t) {
        builder.advanceTo(t);
      },
    };
  };

  return {
    Source() {
      let [node, send, notify] = builder.source(Timestamp(0));
      return [asStream(node), send, notify];
    },
  };
};

module.exports = Query;
