import { makemap } from "./map.js";
import { setup_click } from "./interaction.js";
let map = makemap();
map.on("load", () => {
  // LOAD DATA: add vector tileset from DVRPC's server

  map.addSource("lts_tile", {
    type: "vector",
    url: "https://www.tiles.dvrpc.org/data/lts.json",
    minzoom: 8,
    generateId: true,
  });

  map.addLayer({
    id: "lts",
    type: "line",
    source: "lts_tile",
    "source-layer": "existing_conditions_lts",
    paint: {
      "line-width": 2,
      "line-opacity": 1,
      "line-color": {
        property: "lts_score",
        stops: [
          [1, "light green"],
          [2, "green"],
          [3, "yellow"],
          [4, "red"],
        ],
      },
    },
  });

  map.addLayer({
    id: "clicked",
    type: "line",
    source: "lts_tile",
    "source-layer": "existing_conditions_lts",
    paint: {
      "line-width": 5,
      "line-opacity": [
        "case",
        ["boolean", ["feature-state", "click"], true],
        0,
        1,
      ],
      "line-color": "light blue",
    },
  });

  setup_click(map);
});

export { map };
