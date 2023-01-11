const makemap = () => {
  // Step 1: create the "map" object
  // -------------------------------
  mapboxgl.accessToken =
    "pk.eyJ1IjoiZHZycGNvbWFkIiwiYSI6ImNrczZlNDBkZzFnOG0ydm50bXR0dTJ4cGYifQ.VaJDo9EtH2JyzKm3cC0ypA";

  return new mapboxgl.Map({
    container: "map",
    style: "mapbox://styles/dvrpcomad/ckz2yw8nu000y14mkkuzicgwn",
    center: [-75.16362, 39.95238],
    zoom: 9.5,
  });
};

export { makemap };
