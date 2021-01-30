const { Builder } = require("./graph");
const {
  Summary,
  summaryPlus,
  tsLess,
  MaxTimestamp,
  Timestamp,
  apply,
} = require("./reachability");

let DataflowBuilder = () => {
  let vertices = [];
  let builder = Builder();
  let matrix;
  let reach;
  let reachable = () => {
    let vec = [];
    for (let v of vertices) {
      let smallest = MaxTimestamp;
      for (let m of v.inbox) {
        if (tsLess(m.t, smallest)) {
          smallest = m.t;
        }
      }
      vec.push(smallest);
    }
    let reach = [];
    for (let i = 0; i < matrix.length; i++) {
      let min = vec[i];
      for (let j = 0; j < vec.length; j++) {
        let result = apply(vec[j], matrix[i][j]);
        if (tsLess(result, min)) {
          min = result;
        }
        if (vertices[j].cap !== null && vertices[j].pending > 0) {
          let result = apply(vertices[j].cap, matrix[i][j]);
          if (tsLess(result, min)) {
            min = result;
          }
        }
      }
      reach.push(min);
    }
    // console.log(matrix);
    // console.log("vec", vec);
    // console.log("reach", reach);
    // console.log(
    //   "inbox sizes",
    //   vertices.map((v) => v.inbox.length)
    // );
    // console.log(
    //   "caps",
    //   vertices.map((v) => v.cap)
    // );
    return reach;
  };

  let maybeNotify = () => {
    reach = reachable();
    for (let i = 0; i < vertices.length; i++) {
      if (
        vertices[i].notifiedOf === null ||
        tsLess(vertices[i].notifiedOf, reach[i])
      ) {
        vertices[i].notifiedOf = reach[i];
        vertices[i].impl.onNotify(reach[i]);
      }
    }
  };

  let self = {
    send(from, port, msg, t) {
      for (let { to, toPort } of vertices[from].edges[port] || []) {
        vertices[to].inbox.push({
          type: "row",
          msg,
          t,
          port: toPort,
        });
      }
    },
    notify(from, t) {
      let v = vertices[from];
      if (v.cap === null || tsLess(v.cap, t)) {
        v.cap = t;
      }
      maybeNotify();
    },
    pending(from, count) {
      vertices[from].pending += count;
    },
    edge(from, fromPort, to, toPort, cost = Summary(0, 0, [])) {
      let v = vertices[from];
      builder.edge(v.id, vertices[to].id, cost);
      while (v.edges.length <= fromPort) {
        v.edges.push([]);
      }
      v.edges[fromPort].push({
        to,
        toPort,
      });
    },
    source(defaultCap, name = "") {
      let v = vertices.length;
      vertices.push({
        name,
        id: builder.node(name),
        impl: { recv() {}, onNotify() {} },
        inbox: [],
        edges: [],
        cap: defaultCap,
        pending: Infinity,
        notifiedOf: null,
      });
      return [v, self.send.bind(this, v, 0), self.notify.bind(this, v)];
    },
    vertex(defaultCap, defn = () => null, name = "") {
      let v = vertices.length;
      let send = self.send.bind(this, v);
      let notify = self.notify.bind(this, v);
      let pending = self.pending.bind(this, v);
      vertices.push({
        name,
        id: builder.node(name),
        impl: {
          recv(e, m, t) {
            send(0, m, t);
          },
          onNotify(t) {
            notify(t);
          },
          ...defn(send, notify, pending),
        },
        inbox: [],
        edges: [],
        cap: defaultCap,
        pending: 0,
        notifiedOf: null,
      });
      return v;
    },
    build() {
      matrix = builder.build();
    },
    advanceTo(t) {
      reach = reachable();
      let anyLess = () => {
        for (let r of reach) {
          if (tsLess(r, t)) {
            return true;
          }
        }
        return false;
      };
      while (anyLess()) {
        self.step();
      }
    },
    step() {
      if (!matrix) {
        throw new Error("must build first");
      }
      for (let v of vertices) {
        // TODO: make the inboxes more efficient.
        if (v.inbox.length > 0) {
          let msg = v.inbox.shift();
          v.impl.recv(msg.port, msg.msg, msg.t);
        }
      }

      maybeNotify();
    },
  };

  return self;
};

module.exports = { DataflowBuilder };
