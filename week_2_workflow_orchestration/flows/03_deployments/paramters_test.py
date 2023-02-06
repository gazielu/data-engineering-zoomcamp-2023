from prefect import task, Flow


@task
def extract_live_data(airport, radius, ref_data):
    # Get the live aircraft vector data around the given airport (or none)
    area = None
    if airport:
        airport_data = ref_data.airports[airport]
        airport_position = aclib.Position(
            lat=float(airport_data["latitude"]), long=float(airport_data["longitude"])
        )
        area = aclib.bounding_box(airport_position, radius)

    print("fetching live aircraft data...")
    raw_aircraft_data = aclib.fetch_live_aircraft_data(area=area)

    return raw_aircraft_data

with Flow("Aircraft-ETL") as flow:
    airport = Parameter("airport", default="IAD")
    radius = Parameter("radius", default=200)

    reference_data = extract_reference_data()
    live_data = extract_live_data(airport, radius, reference_data)

    transformed_live_data = transform(live_data, reference_data)

    load_reference_data(reference_data)
    load_live_data(transformed_live_data)

# Run the Flow with default airport=IAD & radius=200


# ...default radius and a different airport!


if __name__ == "__main__":
    flow.run()
    flow.run(airport="DCA")