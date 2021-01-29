const assert = require("assert");
let Query = require("./query");
let { Timestamp, Summary } = require("./reachability");

describe("query", () => {
  it.skip("lets you write a query", () => {
    let q = Query();
    let [node, send, notify] = q.Source();

    let sel = node
      .select((x) => x % 2 === 0)
      .map((x) => x * 5)
      .fence()
      .inspect((x) => console.log(x));

    sel.Build();

    for (let i = 0; i < 10; i++) {
      send(i, Timestamp(0));
      send(i * 10, Timestamp(1));
    }
    notify(Timestamp(1));
    sel.RunTo(Timestamp(1));
    notify(Timestamp(2));
    sel.RunTo(Timestamp(2));
  });

  it("can iterate", () => {
    let q = Query();
    let [node, send, notify] = q.Source();

    let out = [];
    let graph = node
      .iterate((v) => v.map((x) => x * 2).select((x) => x < 2000))
      .inspect((x) => out.push(x));

    graph.Build();

    send(1, Timestamp(0));
    notify(Timestamp(1));
    graph.RunTo(Timestamp(1));

    assert.deepStrictEqual(out, [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]);
  });

  it("runs distinct", () => {
    let q = Query();
    let [node, send, notify] = q.Source();

    let out = [];
    let graph = node.distinct().inspect((x) => out.push(x));

    graph.Build();

    send(1, Timestamp(0));
    send(1, Timestamp(0));
    send(2, Timestamp(0));
    send(3, Timestamp(0));
    send(3, Timestamp(0));

    notify(Timestamp(1));
    graph.RunTo(Timestamp(1));

    assert.deepStrictEqual([1, 2, 3], out);
  });

  it("joins", () => {
    let q = Query();
    let [l, sendL, notifyL] = q.Source();
    let [r, sendR, notifyR] = q.Source();

    let out = [];
    let graph = l.join(r).inspect((x) => out.push(x));

    graph.Build();

    sendL(1, Timestamp(0));
    sendR(1, Timestamp(0));
    sendR(2, Timestamp(0));
    sendL(2, Timestamp(0));

    notifyL(Timestamp(1));
    notifyR(Timestamp(1));
    graph.RunTo(Timestamp(1));

    assert.deepStrictEqual(
      [
        [1, 1],
        [2, 2],
      ],
      out
    );
  });

  it("does a graph", () => {
    let q = Query();

    let edges = [
      [1, 2],
      [2, 3],
      [3, 4],
      [5, 6],
    ];

    let [edgeColl, sendEdge, notifyEdge] = q.Source();
    let [vertColl, sendVert, notifyVert] = q.Source();

    let out = [];
    let graph = vertColl
      .iterate((v) =>
        v
          .concat(
            v.join(
              edgeColl,
              (x) => x,
              (e) => e[0],
              (_, e) => e[1]
            )
          )
          .distinct()
      )
      .inspect((x) => out.push(x));

    graph.Build();

    for (let e of edges) {
      sendEdge(e, [0, [0]]);
    }
    sendVert(1, Timestamp(0));

    notifyEdge([1, [0]]);
    notifyVert(Timestamp(1));
    graph.RunTo(Timestamp(1));

    out.sort();

    assert.deepStrictEqual(out, [1, 2, 3, 4]);
  });
});
