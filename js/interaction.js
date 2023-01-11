const featurelist = [];

const setup_click = (map) => {
  map.on("click", "lts", (e) => {
    e.features.forEach((feature) => {
      if (featurelist.includes(`${feature.properties.id}`)) {
        var index = featurelist.indexOf(`${feature.properties.id}`);
        if (index > -1) {
          // only splice array when item is found
          featurelist.splice(index, 1); // 2nd parameter means remove one item only
        }
      } else {
        featurelist.push(`${feature.properties.id}`);
        map.setFeatureState(
          {
            source: "lts_tile",
            id: feature.properties.id,
            sourceLayer: "clicked",
          },
          { click: false }
        );
      }
      document.getElementById("segids").innerHTML = `${featurelist}`;
    });
    console.log(featurelist);
  });
};
export { setup_click };
