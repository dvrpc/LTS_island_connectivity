import { makemap } from "./map.js";
import { setup_click, clickClear } from "./interaction.js";
let map = makemap();
map.on("load", () => {
  // LOAD DATA: add vector tileset from DVRPC's server

  map.addSource("lts_tile", {
    type: "vector",
    url: "https://www.tiles.dvrpc.org/data/lts.json",
    minzoom: 8,
    promoteId: "id",
  });


  map.addSource("sw_tile", {
    type: "vector",
    url: "https://www.tiles.dvrpc.org/data/pedestrian-network.json",
    minzoom: 8,
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
          [1, "green"],
          [2, "light green"],
          [3, "yellow"],
          [4, "red"],
        ],
      },
    },
  },
    // add layer before road intersection layer
    'road-intersection'
  );

  map.addLayer({
    id: "clicked",
    type: "line",
    source: "lts_tile",
    "source-layer": "existing_conditions_lts",
    paint: {
      "line-width": 15,
      "line-opacity": [
        "case",
        ["boolean", ["feature-state", "click"], false],
        0.7,
        0,
      ],
      "line-color": "white",
    },
  });

  map.addLayer({
    id: "sw",
    type: "line",
    source: "sw_tile",
    "source-layer": "ped_lines",
    paint: {
      "line-width": 2,
      "line-opacity": 1,
      "line-color": "blue"
    },
  },
    'road-intersection'
  );

  setup_click(map);
  clickClear(map);

});

const tilesLoaded = map.areTilesLoaded();
console.log(tilesLoaded)
export { map };
