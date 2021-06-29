## Data dictionary

> dateDim table

|Columns|Type|Nullable|Description|
|-------|----|--------|------|
|dateid|varchar|false|sha1 hash of the date string|
|date|varchar|false|Full date in the form of `YYYY/MM/DD`|
|year|int4|false|Year part of the date|
|month|int4|false|Month part of the date|
|day|int4|false|Day part of the date|
|dayofweek|int4|false|Day of the week|
|weekofyear|int4|false|Week of the year|

> locationDim table

|Columns|Type|Nullable|Description|
|-------|----|--------|------|
|locationId|Varchar|false|sha1 hash of the concatenation of city and country columns|
|city|Varchar|false|Name of a city|
|State|Varchar|false|Name of a state|
|country|Varchar|false|Name of a country|
|latitude|Varchar|false|Latitude coordinate of the city|
|longitude|Varchar|false|Longitude coordinate of the city|
|totalPopulation|Varchar|false|Total population of a city|

> temperatureFact table

|Columns|Type|Nullable|Description|
|-------|----|--------|------|
|temperatureId|int8|false|Sequential ID of each record|
|dateId|Varchar|false|Date ID as foregin key to the dateDim table
|locationId|Varchar|false|Location ID as foregin key to the locationDim table|
|avgTemp|numeric|false|Average daily temperature of each location on a particular day|
|avgTempUncert|numeric|false|Average daily temperature uncertainty on a particular day|