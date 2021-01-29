const { DataflowBuilder } = require("./push");
const { Timestamp, I, E, F, apply, tsLess } = require("./reachability");

describe("push", () => {
  it("runs", () => {
    let d = DataflowBuilder();

    let [a, send, notify] = d.source();

    let b = d.vertex((send, notify) => ({
      recv(e, m, t) {
        if (m % 10 !== 9) {
          send(0, m + 1, t);
        }
      },
      onNotify(t) {
        notify(t);
      },
    }));

    let out = [];
    let c = d.vertex((send, notify) => ({
      recv(e, m, t) {
        out.push([m, t]);
      },
      onNotify(t) {
        console.log("notified of", t);
        let newOut = [];
        for (let [m, ts] of out) {
          if (tsLess(ts, t)) {
            console.log(m);
          } else {
            newOut.push([m, ts]);
          }
        }
        out = newOut;
        notify(t);
      },
    }));

    let i = d.vertex((send, notify) => ({
      recv(e, m, t) {
        send(0, m, apply(t, I));
      },
      onNotify(t) {
        notify(t);
      },
    }));

    let f = d.vertex((send, notify) => ({
      recv(e, m, t) {
        send(0, m, apply(t, F));
      },
      onNotify(t) {
        notify(t);
      },
    }));

    let e = d.vertex((send, notify) => ({
      recv(e, m, t) {
        send(0, m, apply(t, E));
      },
      onNotify(t) {
        notify(t);
      },
    }));

    d.edge(a, 0, i, 0);
    d.edge(i, 0, b, 0, I);
    d.edge(b, 0, f, 0);
    d.edge(f, 0, b, 0, F);
    d.edge(b, 0, e, 0);
    d.edge(e, 0, c, 0, E);

    d.build();

    send(1, Timestamp(0));
    send(10, Timestamp(1));
    send(100, Timestamp(2));
    notify(Timestamp(10));

    for (let i = 1; i < 10; i++) {
      console.log("advancing to", i);
      d.advanceTo(Timestamp(i));
    }

    console.log(out);

    // let a = d.Collection(
    //   { value: 1, t: Timestamp(1) },
    //   { value: 10, t: Timestamp(10) },
    //   { value: 100, t: Timestamp(100) }
    // );
  });
});
