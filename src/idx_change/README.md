The idea of this section is to calculate index changes between 2019 and 2023 using the Sentinel-2 composites generated in the previous step.

The script uses ee assets stored in EE to load the masks for roads buffers (overture + Bing) and calculate index changes over :
- the entire municipality area
- the road categories (which differentiate amogst  trunk, primary, secondary, other both in urban and rural areas)
    - assets path example : 'projects/small-towns-col/assets/muni_road_geometries/municipality_17614'
- index in rural only. using mask in : 'projects/small-towns-col/assets/mun2018_nocabeceras_simpl10' 
- index in main urban ('cabeceras') only. using mask in : '"projects/small-towns-col/assets/col_zon_urb_sel"'
    - example code :
    ``` javascript    
    var urb = ee.FeatureCollection("projects/small-towns-col/assets/col_zon_urb_sel");

    var urb = urb
    .filter(ee.Filter.eq('selected_m',true))
    . filter(ee.Filter.eq('clas_ccdgo', '1' ))
    ;
    ```

The indices calculated are oriented to determine upgrades over roads :

- BSI : Bare Soil Index - (primary)
- NDBI : Normalized Difference Built-up Index (primary)
- NDVI : Normalized Difference Vegetation Index
- SAVI : Soil Adjusted Vegetation Index
- NDMI : Normalized Difference Moisture Index
- NDWI : Normalized Difference Water Index
- MNDWI : Modified Normalized Difference Water Index

The ultimate outcome is a CSV file with the index changes for each municipality, road category and area type (urban/rural). 
Intermediate assrts might need to be stored in GCP.