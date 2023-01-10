const setup_hover = (map) => {
  map.on("click", "lts", (e) => {
    let featurelist = [];
    e.features.forEach((feature) => {
      featurelist.push(`${feature.properties.id}`);
    });
    console.log(featurelist);
    document.getElementById(
      "box"
    ).innerHTML = `Segment ids: <br> ${featurelist}`;
  });
};

export { setup_hover };
