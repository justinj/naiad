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

  it("does a differential distinct", () => {
    let q = Query();
    let [node, send, notify] = q.Source();

    let out = [];
    let sel = node
      .diffDistinct({ diffIsZero: (a) => a === 0 })
      .inspect((x) => out.push(x));

    sel.Build();

    send([1, 1], Timestamp(0));
    send([1, 1], Timestamp(0));
    send([1, 1], Timestamp(0));

    // notify(Timestamp(1));
    // sel.RunTo(Timestamp(1));
    // console.log("removed zero");
    // console.log(out);

    // send([1, -1], Timestamp(1));
    // console.log("removed one");
    // out = [];
    // notify(Timestamp(2));
    // sel.RunTo(Timestamp(2));
    // console.log(out);
    // console.log("removed two");
    // out = [];
    // send([1, -1], Timestamp(2));
    // notify(Timestamp(3));
    // sel.RunTo(Timestamp(3));
    // console.log(out);
    // console.log("removed three");
    // out = [];
    // send([1, -1], Timestamp(3));
    // notify(Timestamp(4));
    // sel.RunTo(Timestamp(4));
    // console.log(out);

    // send([1, 1], Timestamp(1));
    // notify(Timestamp(2));
    // sel.RunTo(Timestamp(2));
  });

  it("reduces", () => {
    let q = Query();
    let [node, send, notify] = q.Source();

    let out = [];
    let sel = node
      .reduce(
        () => true,
        (a, b) => a + b
      )
      .inspect((x) => out.push(x));

    sel.Build();

    send([1, 1], Timestamp(0));
    send([2, 1], Timestamp(0));
    send([1, 1], Timestamp(0));
    send([2, -1], Timestamp(0));
    send([1, 1], Timestamp(0));

    notify(Timestamp(1));
    sel.RunTo(Timestamp(1));

    send([1, 1], Timestamp(1));
    notify(Timestamp(2));
    sel.RunTo(Timestamp(2));

    assert.deepStrictEqual(out, [
      [[true, 3], 1],
      [[true, 3], -1],
      [[true, 4], 1],
    ]);
  });

  it("consolidates", () => {
    let q = Query();
    let [node, send, notify] = q.Source();

    let out = [];
    let sel = node
      .consolidate({
        diffAdd: (a, b) => a + b,
        diffIsZero: (a) => a === 0,
        hash: (a) => a.toString(),
      })
      .inspect((x) => out.push(x));

    sel.Build();

    send([1, 1], Timestamp(0));
    send([2, 1], Timestamp(0));
    send([1, 1], Timestamp(0));
    send([2, -1], Timestamp(0));
    send([1, 1], Timestamp(0));
    send([1, -1], Timestamp(1));

    notify(Timestamp(1));
    sel.RunTo(Timestamp(1));

    assert.deepStrictEqual(out, [[1, 3]]);
    out = [];

    notify(Timestamp(2));
    sel.RunTo(Timestamp(2));

    assert.deepStrictEqual(out, [[1, -1]]);
  });

  it("does a graph differentially", () => {
    let q = Query();

    let edges = [
      [1, 2],
      [2, 3],
      [3, 4],
      [5, 6],
    ];

    let [edgeColl, sendEdge, notifyEdge] = q.Source();
    let [vertColl, sendVert, notifyVert] = q.Source();

    let graph = vertColl
      .iterate((reach) =>
        reach
          .join(
            reach.enter(edgeColl),
            (x) => x[0],
            (e) => e[0][0],
            ([_, m1], [e, m2]) => [e[1], m1 * m2]
          )
          // .inspect(([x, m]) => console.log(`join out ${x}:${m}`))
          .diffDistinct({ diffIsZero: (a) => a === 0 })
      )
      .concat(vertColl)
      .inspect((x) => console.log("reachable:", x));

    graph.Build();

    for (let e of edges) {
      console.log("adding edge", e);
      sendEdge([e, 1], Timestamp(0));
    }
    console.log("roots: 1");
    sendVert([1, 1], Timestamp(0));

    notifyEdge(Timestamp(1));
    notifyVert(Timestamp(1));
    graph.RunTo(Timestamp(1));

    console.log("...");

    console.log("adding edge [4,5]");
    sendEdge([[4, 5], 1], Timestamp(1));
    notifyEdge(Timestamp(2));
    notifyVert(Timestamp(2));
    graph.RunTo(Timestamp(2));

    console.log("...");

    console.log("removing edge [4,5]");
    sendEdge([[4, 5], -1], Timestamp(2));
    notifyEdge(Timestamp(3));
    notifyVert(Timestamp(3));
    graph.RunTo(Timestamp(3));

    // assert.deepStrictEqual(out.map(([a]) => a), [1, 2, 3, 4]);
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
              v.enter(edgeColl),
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
      sendEdge(e, Timestamp(0));
    }
    sendVert(1, Timestamp(0));

    notifyEdge(Timestamp(1));
    notifyVert(Timestamp(1));
    graph.RunTo(Timestamp(1));

    out.sort();

    assert.deepStrictEqual(out, [1, 2, 3, 4]);
  });
});
