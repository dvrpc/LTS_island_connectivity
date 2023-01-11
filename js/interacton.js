const featurelist = [];

function removeValue(feature, index, arr) {
  // If the value at the current array index matches the specified value (2)
  if (feature === 2) {
    // Removes the value from the original array
    arr.splice(index, 1);
    return true;
  }
  return false;
}

const setup_click = (map) => {
  map.on("click", "lts", (e) => {
    e.features.forEach((feature) => {
      if (featurelist.includes(`${feature.properties.id}`)) {
        console.log(`okay dummy, ${feature.properties.id} needs to go.`);
        var index = featurelist.indexOf(`${feature.properties.id}`);
        if (index > -1) {
          // only splice array when item is found
          featurelist.splice(index, 1); // 2nd parameter means remove one item only
        }
      } else {
        featurelist.push(`${feature.properties.id}`);
        console.log(feature);
      }
      document.getElementById("segids").innerHTML = `${featurelist}`;
      console.log(featurelist);
    });
  });
};
export { setup_click };
