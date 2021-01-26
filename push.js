const { Builder } = require("./graph");
const { Summary } = require("./reachability");

let DataflowBuilder = () => {
  let vertices = [];
  let builder = Builder();
  let self = {
    send(from, port, msg, t) {
      for (let { to, toPort } of vertices[from].edges[port]) {
        vertices[to].inbox.push({
          type: "row",
          msg,
          t,
          port: toPort,
        });
      }
    },
    notify(from) {},
    edge(from, fromPort, to, toPort, cost = Summary(0, 0, [])) {
      let v = vertices[from];
      builder.edge(vertices[from].id, vertices[to].id, cost);
      while (v.edges.length <= fromPort) {
        v.edges.push([]);
      }
      v.edges[fromPort].push({
        to,
        toPort,
      });
    },
    source(name = "") {
      let v = vertices.length;
      vertices.push({
        name,
        id: builder.node(name),
        impl: {},
        inbox: [],
        edges: [],
      });
      return [v, self.send.bind(this, v, 0), self.notify.bind(this, v)];
    },
    vertex(defn, name = "") {
      let v = vertices.length;
      let send = self.send.bind(this, v);
      let notify = self.notify.bind(this, v);
      vertices.push({
        name,
        id: builder.node(name),
        impl: defn(send, notify),
        inbox: [],
        edges: [],
      });
      return v;
    },
    run() {
      let matrix = builder.build();
      for (let v of vertices) {
        // TODO: make the inboxes more efficient.
        if (v.inbox.length > 0) {
          let msg = v.inbox.shift();
          console.log(msg);
          v.impl.recv(msg.port, msg.msg, msg.t);
        }
      }
    },
  };

  return self;
};

module.exports = { DataflowBuilder };
