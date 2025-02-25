# Will accept a range of years and aggregate the data for each year
# into a single file. The data will be stored in a directory called
# fred_data/aggregated_data/indicator={indicator}/year={year}
# The data will be stored in a Parquet file called {indicator}_{year}.parquet

# Will get a list of years from the range provided
# Will then grab all months for each year for each indicator and aggregate the data

# Aggregated Data
#{"indicator":"DGS10","observation_year":2018,"observation_month":10,"value":3.15,"observation_count":22,"processing_timestamp":"2025-02-02T08:47:36.533475","aggregation_timestamp":"2025-02-02T08:48:37.849381"}
# {"indicator":"DGS10","observation_year":2018,"observation_month":1,"value":2.58,"observation_count":21,"processing_timestamp":"2025-02-02T08:47:17.767871","aggregation_timestamp":"2025-02-02T08:48:37.849381"}
# {"indicator":"DGS10","observation_year":2018,"observation_month":11,"value":3.12,"observation_count":20,"processing_timestamp":"2025-02-02T08:47:37.946458","aggregation_timestamp":"2025-02-02T08:48:37.849381"}
# {"indicator":"DGS10","observation_year":2018,"observation_month":12,"value":2.83,"observation_count":19,"processing_timestamp":"2025-02-02T08:47:39.353138","aggregation_timestamp":"2025-02-02T08:48:37.849381"}
# {"indicator":"DGS10","observation_year":2018,"observation_month":2,"value":2.86,"observation_count":19,"processing_timestamp":"2025-02-02T08:47:23.509420","aggregation_timestamp":"2025-02-02T08:48:37.849381"}
# {"indicator":"DGS10","observation_year":2018,"observation_month":3,"value":2.84,"observation_count":21,"processing_timestamp":"2025-02-02T08:47:25.459950","aggregation_timestamp":"2025-02-02T08:48:37.849381"}
# {"indicator":"DGS10","observation_year":2018,"observation_month":6,"value":2.91,"observation_count":21,"processing_timestamp":"2025-02-02T08:47:30.537939","aggregation_timestamp":"2025-02-02T08:48:37.849381"}
# {"indicator":"DGS10","observation_year":2018,"observation_month":8,"value":2.89,"observation_count":23,"processing_timestamp":"2025-02-02T08:47:33.772190","aggregation_timestamp":"2025-02-02T08:48:37.849381"}
# {"indicator":"DGS10","observation_year":2018,"observation_month":9,"value":3,"observation_count":19,"processing_timestamp":"2025-02-02T08:47:35.168032","aggregation_timestamp":"2025-02-02T08:48:37.849381"}
# {"indicator":"DGS10","observation_year":2018,"observation_month":4,"value":2.87,"observation_count":21,"processing_timestamp":"2025-02-02T08:47:27.236196","aggregation_timestamp":"2025-02-02T08:48:37.849381"}
# {"indicator":"DGS10","observation_year":2018,"observation_month":5,"value":2.98,"observation_count":22,"processing_timestamp":"2025-02-02T08:47:28.871621","aggregation_timestamp":"2025-02-02T08:48:37.849381"}
# {"indicator":"DGS10","observation_year":2018,"observation_month":7,"value":2.89,"observation_count":21,"processing_timestamp":"2025-02-02T08:47:32.207487","aggregation_timestamp":"2025-02-02T08:48:37.849381"}
