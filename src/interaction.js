const featurelist = [];
var clicked = false

const setup_click = (map) => {
  map.on("click", "clicked", (e) => {
    e.features.forEach((feature) => {
      if (featurelist.includes(feature.id)) {
        var index = featurelist.indexOf(feature.id);
        if (index > -1) {
          featurelist.splice(index, 1);
        }
        map.setFeatureState(
          {
            source: "lts_tile",
            id: feature.id,
            sourceLayer: "existing_conditions_lts",
          },
          { click: false }
        );
      } else {
        featurelist.push(feature.id);
        map.setFeatureState(
          {
            source: "lts_tile",
            id: feature.id,
            sourceLayer: "existing_conditions_lts",
          },
          { click: true }
        );
      }
      document.getElementById("segids").innerHTML = `${featurelist}`;
    });
  });
};

function clickClear() {
  document.getElementById('clear_button').addEventListener("click", function() {
    clicked = true
    console.log(clicked)
    document.getElementById("segids").innerHTML = [];
    return clicked
  });
}


export { setup_click, clickClear };
