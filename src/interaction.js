var featurelist = [];

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

const clickClear = (map) => {
  document.getElementById('clear_button').addEventListener("click", function() {
    console.log(featurelist)
    document.getElementById("segids").innerHTML = [];
    for (const element of featurelist) {
      map.setFeatureState(
        {
          source: "lts_tile",
          id: element,
          sourceLayer: "existing_conditions_lts",
        },
        { click: false }
      );
    }

    featurelist = []
  });
}


export { setup_click, clickClear };
