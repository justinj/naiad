const { Builder } = require("./graph");
const { I, E, F, writeSummary } = require("./reachability");

describe("graphs", () => {
  it("works", () => {
    let builder = Builder();

    let a = builder.node();
    let i = builder.node();
    let b = builder.node();
    let f = builder.node();
    let e = builder.node();
    let c = builder.node();

    builder.edge(a, i);
    builder.edge(i, b, I);
    builder.edge(b, e);
    builder.edge(e, c, E);
    builder.edge(b, f);
    builder.edge(f, b, F);

    let mat = builder.build();
    for (let row of mat) {
      console.log(row.map(writeSummary));
    }
  });
});
