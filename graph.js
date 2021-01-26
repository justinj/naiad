const { Summary, less, summaryPlus, Unreachable } = require("./reachability");

const Zero = Summary(0, 0, []);

const meet = (a, b) => {
  if (a === Unreachable) {
    return b;
  }
  if (b === Unreachable) {
    return a;
  }
  return less(a, b) ? a : b;
};

const matPlus = (a, b) => {
  let mat = [];
  for (let y = 0; y < a.length; y++) {
    let row = [];
    for (let x = 0; x < a[y].length; x++) {
      row.push(meet(a[y][x], b[y][x]));
    }
    mat.push(row);
  }
  return mat;
};

const matTimes = (a, b) => {
  let n = a.length;
  let mat = new Array(n).fill(null).map(() => new Array(n).fill(Unreachable));
  for (let i = 0; i < n; i++) {
    for (let j = 0; j < n; j++) {
      for (let k = 0; k < n; k++) {
        mat[i][j] = meet(mat[i][j], summaryPlus(b[k][j], a[i][k]));
      }
    }
  }
  return mat;
};

const Builder = () => {
  let matrix = [];
  let names = [];
  return {
    node(name = "") {
      let id = names.length;
      names.push(name);

      for (let m of matrix) {
        m.push(Unreachable);
      }
      matrix.push(new Array(matrix.length + 1).fill(Unreachable));

      return id;
    },
    edge(from, to, cost = Zero) {
      matrix[to][from] = meet(matrix[to][from], cost);
    },
    build() {
      let result = matrix;
      let mul = matrix;
      for (let i = 0; i < matrix.length; i++) {
        mul = matTimes(mul, matrix);
        result = matPlus(result, mul);
      }
      return result;
    },
  };
};

module.exports = {
  Builder,
};
