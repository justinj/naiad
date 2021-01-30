const { send } = require("process");
const { DataflowBuilder } = require("./push");
const { tsMin, tsLess, I, E, F, apply, Timestamp } = require("./reachability");
const Query = () => {
  let builder = DataflowBuilder();

  const wrap = (node) => {
    let forEach = (f) => {
      let next = builder.vertex((send, notify) => ({
        recv(e, m, t) {
          f(send, e, m, t);
        },
        onNotify(t) {
          notify(t);
        },
      }));
      builder.edge(node, 0, next, 0);

      return wrap(next);
    };
    let flatMap = (f) =>
      forEach((send, e, m, t) => {
        for (let x of f(m)) {
          send(e, x, t);
        }
      });

    let concat = (other) => {
      let next = builder.vertex((send, notify) => ({
        recv(e, m, t) {
          send(0, m, t);
        },
        onNotify(t) {
          notify(t);
        },
      }));
      builder.edge(node, 0, next, 0);
      builder.edge(other.node, 0, next, 1);

      return wrap(next);
    };
    let passthrough = (s) =>
      builder.vertex((send, notify) => ({
        recv(e, m, t) {
          send(0, m, apply(t, s));
        },
        onNotify(t) {
          notify(apply(t, s));
        },
      }));
    return {
      node,
      flatMap,
      select(f) {
        return flatMap((m) => (f(m) ? [m] : []));
      },
      map(f) {
        return flatMap((m) => [f(m)]);
      },
      concat,
      iterate(other) {
        builder.enter();
        let i = passthrough(I);
        let f = passthrough(F);
        let conc = builder.vertex((send, notify) => ({
          recv(e, m, t) {
            send(0, m, t);
          },
          onNotify(t) {
            notify(t);
          },
        }));
        let out = builder.vertex((send, notify) => ({
          recv(e, m, t) {
            send(0, m, t);
          },
          onNotify(t) {
            notify(t);
          },
        }));
        let c = builder.vertex((send, notify) => ({
          recv(e, m, t) {
            send(0, m, t);
          },
          onNotify(t) {
            notify(t);
          },
        }));
        let computation = other(wrap(c));

        builder.exit();

        let e = passthrough(E);

        builder.edge(node, 0, i, 0);
        builder.edge(i, 0, conc, 0, I);
        builder.edge(conc, 0, c, 0);
        builder.edge(computation.node, 0, f, 0);
        builder.edge(f, 0, conc, 1, F);
        builder.edge(computation.node, 0, e, 0);
        builder.edge(e, 0, out, 0, E);

        return wrap(out);
      },
      join(
        other,
        leftKey = (x) => x.toString(),
        rightKey = (x) => x.toString(),
        combine = (x, y) => [x, y]
      ) {
        let leftTable = {};
        let rightTable = {};
        let next = builder.vertex((send, notify) => ({
          recv(e, m, t) {
            if (e === 0) {
              let key = leftKey(m);
              if (!leftTable.hasOwnProperty(key)) {
                leftTable[key] = [];
              }
              leftTable[key].push([m, t]);
              for (let [row, ts] of rightTable[key] || []) {
                let outTs = ts;
                if (tsLess(ts, t)) outTs = t;
                send(0, combine(m, row), outTs);
              }
            } else if (e === 1) {
              let key = rightKey(m);
              if (!rightTable.hasOwnProperty(key)) {
                rightTable[key] = [];
              }
              rightTable[key].push([m, t]);
              for (let [row, ts] of leftTable[key] || []) {
                let outTs = ts;
                if (tsLess(ts, t)) outTs = t;
                send(0, combine(row, m), outTs);
              }
            }
          },
          onNotify(ts) {
            notify(ts);
          },
        }));

        builder.edge(node, 0, next, 0);
        builder.edge(other.node, 0, next, 1);

        return wrap(next);
      },
      distinct(hash = (x) => "" + x) {
        let seen = {};
        let queue = [];
        let next = builder.vertex((send, notify, pending) => ({
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

        return wrap(next);
      },
      fence() {
        let out = [];
        let next = builder.vertex((send, notify) => ({
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

        return wrap(next);
      },
      inspect(f) {
        let next = builder.vertex((send, notify) => ({
          recv(e, m, t) {
            f(m);
            send(0, m, t);
          },
          onNotify(t) {
            notify(t);
          },
        }));
        builder.edge(node, 0, next, 0);

        return wrap(next);
      },
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
      let [node, send, notify] = builder.source();
      return [wrap(node), send, notify];
    },
  };
};

module.exports = Query;
