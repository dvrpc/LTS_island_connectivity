const setup_click = (map) => {
  map.on("click", "lts", (e) => {
    let featurelist = [];
    e.features.forEach((feature) => {
      featurelist.push(`${feature.properties.id}`);
    });
    console.log(featurelist);
    document.getElementById("segids").innerHTML = `${featurelist}`;
  });
};
export { setup_click };
