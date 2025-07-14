/* eslint-disable no-await-in-loop */
/* eslint-disable no-restricted-syntax */
/* eslint-disable no-prototype-builtins */

const sharp = require('sharp');
const { pool } = require('./pools');

const {
  debug, sendResults, simpleQuery, safeQuery,
} = require('./database');

/*
  SELECT distinct b.plant_symbol as current_symbol, a.plant_symbol AS state_symbol
  FROM (
    SELECT plant_symbol, state_code, b.* FROM (
      SELECT * FROM mlra_species a
      LEFT JOIN plants3.plant_master_tbl b
      USING (plant_symbol)
    ) a
    JOIN plants3.plant_synonym_tbl b
    ON a.plant_master_id = synonym_plant_master_id
  ) a
  JOIN plants3.plant_master_tbl b
  USING (plant_master_id);

  UPDATE mlra_species SET plant_symbol = 'SYEL2' WHERE plant_symbol = 'SYEL';
  UPDATE mlra_species SET plant_symbol = 'EUFI14' WHERE plant_symbol = 'EUFI2';
  UPDATE mlra_species SET plant_symbol = 'LIPUM2' WHERE plant_symbol = 'LIMU';
  UPDATE mlra_species SET plant_symbol = 'ARTRV' WHERE plant_symbol = 'SEVA';
  UPDATE mlra_species SET plant_symbol = 'GLMA4' WHERE plant_symbol = 'GLSO80';
  UPDATE mlra_species SET plant_symbol = 'SOBID' WHERE plant_symbol = 'SOBIS';
  UPDATE mlra_species SET plant_symbol = 'OEFI3' WHERE plant_symbol = 'GAFI2';
  UPDATE mlra_species SET plant_symbol = 'SCAR7' WHERE plant_symbol = 'SCPH';
  UPDATE mlra_species SET plant_symbol = 'OEGA' WHERE plant_symbol = 'GABI2';
  UPDATE mlra_species SET plant_symbol = 'NEAT' WHERE plant_symbol = 'PAAT';
  UPDATE mlra_species SET plant_symbol = 'EUPU21' WHERE plant_symbol = 'EUPU10';
  UPDATE mlra_species SET plant_symbol = 'TRRI' WHERE plant_symbol = 'TRRI8';
  UPDATE mlra_species SET plant_symbol = 'EUMA9' WHERE plant_symbol = 'EUMA12';
*/

/*
  SELECT * INTO plants3.nativity
  FROM (
    SELECT DISTINCT
      plc.plant_master_id,
      COALESCE(dpn.plant_nativity_id, 0) AS plant_nativity,
      COALESCE(plant_excluded_location_ind, false) AS plant_excluded_ind,
      COALESCE(pl.plant_nativity_region_id, 0) AS plant_nativity_region_id,
      dpnr.plant_nativity_region_name,
      COALESCE(plant_nativity_type, '') AS plant_nativity_type,
      COALESCE(plant_nativity_name, '') AS plant_nativity_name,
      country_identifier,
      ROW_NUMBER() OVER (PARTITION BY plc.plant_master_id, dpnr.plant_nativity_region_name
                        ORDER BY plant_nativity_type, dpnr.plant_nativity_region_name ASC) AS rn
    FROM
      plants3.plant_location_characteristic plc
      INNER JOIN plants3.plant_location pl ON pl.plant_location_id = plc.plant_location_id
      INNER JOIN plants3.d_plant_nativity dpn ON plc.plant_nativity_id = dpn.plant_nativity_id
      INNER JOIN plants3.d_plant_nativity_region dpnr ON pl.plant_nativity_region_id = dpnr.plant_nativity_region_id
    ORDER BY 1
  ) alias;

  CREATE TABLE plants3.states (
    state VARCHAR(255),
    plant_symbol VARCHAR(10),
    cultivar_name VARCHAR(20),
    parameter VARCHAR(50),
    value VARCHAR(255),
    notes TEXT
  );

  CREATE TABLE weather.canada30year (
    lat NUMERIC,
    lon NUMERIC,
    fff_0 NUMERIC,
    fff_2 NUMERIC,
    fff_4 NUMERIC,
    lsf_0 NUMERIC,
    lsf_2 NUMERIC,
    lsf_4 NUMERIC,
    fff_0_date DATE,
    fff_2_date DATE,
    fff_4_date DATE,
    lsf_0_date DATE,
    lsf_2_date DATE,
    lsf_4_date DATE
  );
*/

const routeCharacteristics = async (req, res) => {
  const createCharacteristics = async () => {
    const sq = `
      CREATE TABLE IF NOT EXISTS plants3.plant_master_tbl_backup AS SELECT * FROM plants3.plant_master_tbl;
      CREATE TABLE IF NOT EXISTS plants3.plant_morphology_physiology_backup AS SELECT * FROM plants3.plant_morphology_physiology;
      CREATE TABLE IF NOT EXISTS plants3.plant_growth_requirements_backup AS SELECT * FROM plants3.plant_growth_requirements;
      CREATE TABLE IF NOT EXISTS plants3.plant_reproduction_backup AS SELECT * FROM plants3.plant_reproduction;
      CREATE TABLE IF NOT EXISTS plants3.plant_suitability_use_backup AS SELECT * FROM plants3.plant_suitability_use;
  
      UPDATE plants3.plant_morphology_physiology a
      SET plant_master_id = b.plant_master_id
      FROM plants3.plant_synonym_tbl b
      WHERE a.plant_master_id = b.synonym_plant_master_id;

      UPDATE plants3.plant_growth_requirements a
      SET plant_master_id = b.plant_master_id
      FROM plants3.plant_synonym_tbl b
      WHERE a.plant_master_id = b.synonym_plant_master_id;

      UPDATE plants3.plant_reproduction a
      SET plant_master_id = b.plant_master_id
      FROM plants3.plant_synonym_tbl b
      WHERE a.plant_master_id = b.synonym_plant_master_id;

      UPDATE plants3.plant_suitability_use a
      SET plant_master_id = b.plant_master_id
      FROM plants3.plant_synonym_tbl b
      WHERE a.plant_master_id = b.synonym_plant_master_id;
      
      DROP TABLE IF EXISTS plants3.synonyms;
      SELECT DISTINCT a.*, b.plant_symbol AS psymbol, c.plant_symbol AS ssymbol
      INTO plants3.synonyms
      FROM (
        SELECT a.plant_master_id AS pid, b.synonym_plant_master_id
        FROM plants3.plant_synonym_tbl a
        JOIN plants3.plant_synonym_tbl b
        USING (plant_master_id)
      ) a
      JOIN plants3.plant_master_tbl b
      ON a.pid = b.plant_master_id
      JOIN plants3.plant_master_tbl c
      ON synonym_plant_master_id = c.plant_master_id;
      CREATE INDEX ON plants3.synonyms (pid);
      CREATE INDEX ON plants3.synonyms (synonym_plant_master_id);
            
      DROP TABLE IF EXISTS plants3.characteristics;
      SELECT * INTO plants3.characteristics
      FROM (
        SELECT DISTINCT * FROM (
          SELECT
            p.plant_symbol,
            plant_master_id,
            coalesce(m.cultivar_name, g.cultivar_name, r.cultivar_name, s.cultivar_name) AS cultivar,
            full_scientific_name_without_author,
            primary_vernacular,
            plant_duration_name,
            plant_nativity_type,
            plant_nativity_region_name,
            plant_growth_habit_name,
            cover_crop,
            active_growth_period.season_name AS active_growth_period,
            after_harvest_regrowth_rate.rate_name AS after_harvest_regrowth_rate,
            bloat_potential.extent_name AS bloat_potential,
            c_n_ratio.extent_name AS c_n_ratio,
            coppice_potential_ind,
            fall_conspicuous_ind,
            fire_resistant_ind,
            color_name,
            flower_conspicuous_ind,
            summer.foliage_porosity_name AS summer,
            winter.foliage_porosity_name AS winter,
            foliage_texture_name,
            fruit_seed_conspicuous_ind,
            growth_form_name,
            growth_rate.rate_name AS growth_rate,
            height_max_at_base_age,
            height_at_maturity,
            known_allelopath_ind,
            leaf_retention_ind,
            lifespan_name,
            low_growing_grass_ind,
            nitrogen_fixation_potential.extent_name AS nitrogen_fixation_potential,
            resprout_ability_ind,
            shape_orientation_name,
            toxicity_name,
            
            pd_max_range,
            ff_min_range,
            ph_min_range,
            ph_max_range,
            density_min_range,
            precip_min_range,
            precip_max_range,
            root_min_range,
            temp_min_range,
            commercial_availability_id,
            fruit_seed_abundance_id,
            propagated_by_bare_root_ind,
            propagated_by_bulb_ind,
        
            coarse_texture_soil_adaptable_ind,
            medium_texture_soil_adaptable_ind,
            fine_texture_soil_adaptable_ind,
            anaerobic_tolerance.extent_name AS anaerobic_tolerance,
            caco3_tolerance.extent_name AS caco3_tolerance,
            cold_stratification_required_ind,
            drought_tolerance.extent_name AS drought_tolerance,
            fire_tolerance.extent_name AS fire_tolerance,
            hedge_tolerance.extent_name AS hedge_tolerance,
            moisture_usage.extent_name AS moisture_usage,
            soil_ph_tolerance_min,
            soil_ph_tolerance_max,
            precipitation_tolerance_min, 
            precipitation_tolerance_max,
            salinity_tolerance.extent_name AS salinity_tolerance,
            shade_tolerance_name,
            temperature_tolerance_min,
        
            bloom_period.season_name AS bloom_period,
            fruit_seed_period_start.season_name AS fruit_seed_period_start,
            fruit_seed_period_end.season_name AS fruit_seed_period_end,
            fruit_seed_persistence_ind,
            seed_per_pound,
            seed_spread_rate.rate_name AS seed_spread_rate,
            seedling_vigor.extent_name AS seedling_vigor,
            vegetative_spread_rate.rate_name AS vegetative_spread_rate,
        
            berry_nut_seed_product_ind,
            fodder_product_ind,
            palatability_browse.extent_name AS palatability_browse,
            palatability_graze.extent_name AS palatability_graze,
            palatability_human_ind,
            protein_potential.extent_name AS protein_potential,

            frost_free_days_min,
            planting_density_min,
            root_depth_min,
            vegetation,
            family

          FROM plants3.plant_master_tbl p
          LEFT JOIN plants3.vegetation USING (plant_symbol)
          
          LEFT JOIN plants3.plant_classifications_tbl USING (plant_master_id)
          LEFT JOIN (
            SELECT
              STRING_AGG(plant_duration_name, ', ' ORDER BY plant_duration_name) AS plant_duration_name,
              plant_master_id
            FROM plants3.plant_duration
            LEFT JOIN plants3.d_plant_duration USING (plant_duration_id)
            GROUP BY plant_master_id
          ) pd USING (plant_master_id)
          LEFT JOIN (
            SELECT
              STRING_AGG(plant_growth_habit_name, ', ' ORDER BY plant_growth_habit_name) AS plant_growth_habit_name,
              plant_master_id
            FROM plants3.plant_growth_habit 
            LEFT JOIN plants3.d_plant_growth_habit USING (plant_growth_habit_id)
            GROUP BY plant_master_id
          ) pgh USING (plant_master_id)
          LEFT JOIN plants3.plant_morphology_physiology m USING (plant_master_id)
          LEFT JOIN plants3.plant_growth_requirements g USING (plant_master_id, cultivar_name)
          LEFT JOIN plants3.plant_reproduction r USING (plant_master_id, cultivar_name)
          LEFT JOIN plants3.plant_suitability_use s USING (plant_master_id, cultivar_name)

          LEFT JOIN (
            SELECT
              plant_nativity_type,
              STRING_AGG(nativity.plant_nativity_region_name, ', ' ORDER BY nativity.plant_nativity_region_name) AS plant_nativity_region_name,
              plant_master_id
            FROM plants3.nativity
            LEFT JOIN plants3.d_plant_nativity_region USING (plant_nativity_region_id)
            GROUP BY plant_master_id, plant_nativity_type
          ) nat USING (plant_master_id)

          LEFT JOIN plants3.d_season active_growth_period ON m.active_growth_period_id=active_growth_period.season_id
          LEFT JOIN plants3.d_rate after_harvest_regrowth_rate ON m.after_harvest_regrowth_rate_id=after_harvest_regrowth_rate.rate_id
          LEFT JOIN plants3.d_extent bloat_potential ON m.bloat_potential_id=bloat_potential.extent_id
          LEFT JOIN plants3.d_extent c_n_ratio ON m.c_n_ratio_id=c_n_ratio.extent_id
          LEFT JOIN plants3.d_color c ON m.flower_color_id = c.color_id
          LEFT JOIN plants3.d_foliage_porosity summer ON m.summer_foliage_porosity_id=summer.foliage_porosity_id
          LEFT JOIN plants3.d_foliage_porosity winter ON m.winter_foliage_porosity_id=winter.foliage_porosity_id
          LEFT JOIN plants3.d_foliage_texture foliage_texture_name ON m.foliage_texture_id=foliage_texture_name.foliage_texture_id
          LEFT JOIN plants3.d_growth_form gf ON m.growth_form_id=gf.growth_form_id
          LEFT JOIN plants3.d_rate growth_rate ON m.growth_rate_id=growth_rate.rate_id
          LEFT JOIN plants3.d_lifespan USING (lifespan_id)
          LEFT JOIN plants3.d_extent nitrogen_fixation_potential ON m.nitrogen_fixation_potential_id=nitrogen_fixation_potential.extent_id
          LEFT JOIN plants3.d_shape_orientation q ON m.shape_orientation_id=q.shape_orientation_id
          LEFT JOIN plants3.d_toxicity USING (toxicity_id)
          LEFT JOIN plants3.d_extent anaerobic_tolerance ON g.anaerobic_tolerance_id=anaerobic_tolerance.extent_id
          LEFT JOIN plants3.d_extent caco3_tolerance ON g.caco3_tolerance_id=caco3_tolerance.extent_id
          LEFT JOIN plants3.d_extent drought_tolerance ON g.drought_tolerance_id=drought_tolerance.extent_id
          LEFT JOIN plants3.d_extent fire_tolerance ON g.fire_tolerance_id=fire_tolerance.extent_id
          LEFT JOIN plants3.d_extent hedge_tolerance ON g.hedge_tolerance_id=hedge_tolerance.extent_id
          LEFT JOIN plants3.d_extent moisture_usage ON g.moisture_usage_id = moisture_usage.extent_id
          LEFT JOIN plants3.d_extent salinity_tolerance ON g.salinity_tolerance_id = salinity_tolerance.extent_id
          LEFT JOIN plants3.d_shade_tolerance USING (shade_tolerance_id)
        
          LEFT JOIN plants3.d_season bloom_period ON r.bloom_period_id=bloom_period.season_id
          LEFT JOIN plants3.d_season fruit_seed_period_start ON r.fruit_seed_period_start_id=fruit_seed_period_start.season_id
          LEFT JOIN plants3.d_season fruit_seed_period_end ON r.fruit_seed_period_end_id=fruit_seed_period_end.season_id
          LEFT JOIN plants3.d_rate seed_spread_rate ON r.seed_spread_rate_id=seed_spread_rate.rate_id
          LEFT JOIN plants3.d_extent seedling_vigor ON r.seedling_vigor_id = seedling_vigor.extent_id
          LEFT JOIN plants3.d_rate vegetative_spread_rate ON r.vegetative_spread_rate_id=vegetative_spread_rate.rate_id
        
          LEFT JOIN plants3.d_extent palatability_browse ON s.palatability_browse_id = palatability_browse.extent_id
          LEFT JOIN plants3.d_extent palatability_graze ON s.palatability_graze_id = palatability_graze.extent_id
          LEFT JOIN plants3.d_extent protein_potential ON s.protein_potential_id = protein_potential.extent_id
          WHERE coalesce(
            active_growth_period::text, after_harvest_regrowth_rate::text, bloat_potential::text, c_n_ratio::text, coppice_potential_ind::text,
            fall_conspicuous_ind::text, fire_resistant_ind::text, color_name::text, flower_conspicuous_ind::text, summer::text, winter::text,
            fruit_seed_conspicuous_ind::text, growth_form_name::text, growth_rate::text, height_max_at_base_age::text, height_at_maturity::text,
            known_allelopath_ind::text, leaf_retention_ind::text, lifespan_name::text, low_growing_grass_ind::text, nitrogen_fixation_potential::text,
            resprout_ability_ind::text, shape_orientation_name::text, toxicity_name::text, coarse_texture_soil_adaptable_ind::text,
            medium_texture_soil_adaptable_ind::text, fine_texture_soil_adaptable_ind::text, anaerobic_tolerance::text, caco3_tolerance::text,
            cold_stratification_required_ind::text, drought_tolerance::text, fire_tolerance::text, hedge_tolerance::text, moisture_usage::text,
            soil_ph_tolerance_min::text, soil_ph_tolerance_max::text, precipitation_tolerance_min::text, precipitation_tolerance_max::text,
            salinity_tolerance::text, shade_tolerance_name::text, temperature_tolerance_min::text, bloom_period::text, fruit_seed_period_start::text,
            fruit_seed_period_end::text, fruit_seed_persistence_ind::text, seed_per_pound::text, seed_spread_rate::text, seedling_vigor::text,
            vegetative_spread_rate::text, berry_nut_seed_product_ind::text, fodder_product_ind::text,
            palatability_browse::text, palatability_graze::text,
            palatability_human_ind::text, protein_potential::text,
            plant_nativity_region_name
          ) > ''
          OR p.plant_symbol in (SELECT plant_symbol FROM plants3.states)
        ) alias
        ORDER BY 1, 2, 3
      ) alias;

      -- INSERT INTO plants3.characteristics (plant_symbol, cultivar, plant_nativity_region_name, plant_nativity_type) (
      --   SELECT DISTINCT
      --     a.plant_symbol, a.cultivar_name,
      --     CASE
      --       WHEN state = 'HI' THEN 'Hawaii'
      --       WHEN state = 'AK' THEN 'Alaska'
      --       ELSE 'Lower 48 States'
      --     END,
      --     COALESCE(b.plant_nativity_type, 'Introduced')
      --   FROM plants3.states a
      --   LEFT JOIN plants3.characteristics b
      --   ON a.plant_symbol = b.plant_symbol AND COALESCE(a.cultivar_name, '') = COALESCE(b.cultivar, '')
      --   WHERE b.plant_symbol IS NULL
      -- );

      INSERT INTO plants3.characteristics (plant_symbol, cultivar, plant_nativity_region_name, plant_nativity_type) (
        SELECT DISTINCT
          a.plant_symbol, a.cultivar_name,
          CASE
            WHEN state = 'HI' THEN 'Hawaii'
            WHEN state = 'AK' THEN 'Alaska'
            ELSE 'Lower 48 States'
          END,
          b.plant_nativity_type
        FROM plants3.states a
        LEFT JOIN plants3.characteristics b
        ON a.plant_symbol = b.plant_symbol AND COALESCE(a.cultivar_name, '') = COALESCE(b.cultivar, '')
        WHERE b.plant_symbol IS NULL
      );

      UPDATE plants3.characteristics
      SET cultivar = NULL
      WHERE cultivar = '';

      UPDATE plants3.characteristics a
      SET (
        plant_master_id, full_scientific_name_without_author, primary_vernacular, plant_duration_name, plant_growth_habit_name, cover_crop
      ) =
      ROW(
        b.plant_master_id, b.full_scientific_name_without_author, b.primary_vernacular, b.plant_duration_name, b.plant_growth_habit_name, b.cover_crop
      )
      FROM plants3.characteristics b
      WHERE
        a.plant_symbol = b.plant_symbol
        AND a.full_scientific_name_without_author IS NULL AND b.full_scientific_name_without_author IS NOT NULL;

      UPDATE plants3.states SET state = 'all' WHERE parameter = 'seed_per_pound';

      UPDATE plants3.characteristics a
      SET primary_vernacular = b.value
      FROM plants3.states b
      WHERE
        a.plant_symbol = b.plant_symbol
        AND b.state = 'all'
        AND b.parameter = 'primary_vernacular';

      UPDATE plants3.characteristics a
      SET seed_per_pound = b.value::numeric
      FROM plants3.states b
      WHERE
        a.plant_symbol = b.plant_symbol
        AND a.cultivar = b.cultivar_name
        AND b.state = 'all'
        AND b.parameter = 'seed_per_pound';
        
      CREATE INDEX ON plants3.characteristics (plant_symbol);
      CREATE INDEX ON plants3.characteristics (plant_master_id);
    `;

    await pool.query({ text: sq, multi: true });
  }; // createCharacteristics

  if (req.query.create) {
    await createCharacteristics();
  } else {
    try {
      let results = await pool.query('SELECT COUNT(*) FROM plants3.characteristics');
      if (+results.rows[0].count === 0) {
        await createCharacteristics();
      }

      results = await pool.query('SELECT COUNT(*) FROM plants3.synonyms');
      if (+results.rows[0].count === 0) {
        await createCharacteristics();
      }
    } catch (error) {
      await createCharacteristics();
    }
  }

  let symbols = [];

  const mlra = req.query.mlra || '';
  const state = req.query.state?.toUpperCase() || '';
  const allowedCultivars = {};
  let stateData = [];
  if (state) {
    stateData = (
      await pool.query(`
        SELECT
          state,
          plant_master_id,
          COALESCE(a.plant_symbol, b.plant_symbol) as plant_symbol,
          parameter,
          value,
          cultivar_name,
          sci_name,
          primary_vernacular,
          plant_duration_name,
          plant_nativity_type,
          plant_nativity_region_name,
          plant_growth_habit_name,
          cover_crop
        FROM (
          SELECT
            *,
            CASE
              WHEN state = 'HI' THEN 'Hawaii'
              WHEN state = 'AK' THEN 'Alaska'
              ELSE 'Lower 48 States'
            END AS plant_nativity_region_name
          FROM plants3.states
        ) a
        LEFT JOIN plants3.plant_classifications_tbl b USING (plant_symbol)
        LEFT JOIN plants3.plant_master_tbl USING (plant_master_id)
        LEFT JOIN plants3.plant_duration USING (plant_master_id)
        LEFT JOIN plants3.d_plant_duration USING (plant_duration_id)
        LEFT JOIN plants3.nativity USING (plant_master_id, plant_nativity_region_name)
        LEFT JOIN plants3.plant_growth_habit USING (plant_master_id)
        LEFT JOIN plants3.d_plant_growth_habit USING (plant_growth_habit_id)
        WHERE (
          state = $1
          OR (
            state = 'all' AND CONCAT(a.plant_symbol, a.cultivar_name) IN (
              SELECT CONCAT(plant_symbol, cultivar_name)
              FROM plants3.states
              WHERE state = $1 AND parameter = 'mlra'
            )
          )
        )
        ORDER BY a.plant_symbol, parameter
      `, [state])
    ).rows;

    if (stateData.length) {
      if (mlra) {
        const stateSymbols = stateData
          .filter((row) => row.parameter === 'mlra' && row.value.split(',').includes(mlra))
          .map((row) => row.plant_symbol);

        stateData = stateData.filter((row) => stateSymbols.includes(row.plant_symbol));
      }

      stateData.forEach((row) => {
        if (!symbols.includes(row.plant_symbol)) {
          symbols.push(row.plant_symbol);
        }
        if (row.cultivar_name) {
          allowedCultivars[row.plant_symbol] = allowedCultivars[row.plant_symbol] || [];
          if (!allowedCultivars[row.plant_symbol].includes(row.cultivar_name)) {
            allowedCultivars[row.plant_symbol].push(row.cultivar_name);
          }
        }
      });
    }
  }

  // res.send({ symbols }); return;

  if (mlra && !symbols.length) {
    // from Access database
    symbols = await pool.query(`
      SELECT DISTINCT plant_symbol
      FROM mlra_species
      WHERE mlra='${mlra}'
    `);
    symbols = symbols.rows.map((row) => row.plant_symbol);
  } else if (req.query.symbols) {
    symbols = req.query.symbols.split(',');
  }

  // res.send({ symbols }); return;

  const querySymbols = symbols.map((symbol) => `'${symbol}'`);

  let stateCond = '';
  let regionRegex = 'plant_nativity_region_name';
  let groupBy = '';
  if (state === 'AK') {
    stateCond = ` AND (
                    plant_nativity_region_name ~ 'Alaska'
                    OR (
                      SELECT count(plant_nativity_region_name) FROM plants3.nativity
                      WHERE plant_master_id = characteristics.plant_master_id AND plant_nativity_region_name ~ 'Alaska'
                    ) = 0
                  )
                  AND plant_symbol in (
                    SELECT plant_symbol
                    FROM plants3.states
                    WHERE state = '${state}'
                  )
                `;
    regionRegex = `REGEXP_REPLACE(plant_nativity_region_name, '.*Alaska.*', 'Alaska')`;
  } else if (state === 'HI') {
    stateCond = ` AND (
                    plant_nativity_region_name ~ 'Hawaii'
                    OR (
                      SELECT count(plant_nativity_region_name) FROM plants3.nativity
                      WHERE plant_master_id = characteristics.plant_master_id AND plant_nativity_region_name ~ 'Hawaii'
                    ) = 0
                  )
                  AND plant_symbol in (
                    SELECT plant_symbol
                    FROM plants3.states
                    WHERE state = '${state}'
                  )
                `;
    regionRegex = `REGEXP_REPLACE(plant_nativity_region_name, '.*Hawaii.*', 'Hawaii')`;
  } else if (state) {
    stateCond = ` AND (
                    plant_nativity_region_name ~ 'Lower 48'
                    OR (
                      SELECT count(plant_nativity_region_name) FROM plants3.nativity
                      WHERE plant_master_id = characteristics.plant_master_id AND plant_nativity_region_name ~ 'Lower 48'
                    ) = 0
                  )
                  AND plant_symbol in (
                    SELECT plant_symbol
                    FROM plants3.states
                    WHERE state = '${state}'
                  )
                `;

    regionRegex = `REGEXP_REPLACE(plant_nativity_region_name, '.*Lower 48 States.*', 'Lower 48 States')`;
  }

  const columns = `
    plant_symbol,plant_master_id,cultivar,full_scientific_name_without_author,primary_vernacular,plant_duration_name,
    ${state ? `STRING_AGG(plant_nativity_type, ', ' ORDER BY plant_nativity_type) AS plant_nativity_type` : 'plant_nativity_type'},
    ${regionRegex} AS plant_nativity_region_name,
    plant_growth_habit_name,cover_crop,active_growth_period,after_harvest_regrowth_rate,bloat_potential,c_n_ratio,
    coppice_potential_ind,fall_conspicuous_ind,fire_resistant_ind,color_name,flower_conspicuous_ind,summer,winter,foliage_texture_name,
    fruit_seed_conspicuous_ind,growth_form_name,growth_rate,height_max_at_base_age,height_at_maturity,known_allelopath_ind,leaf_retention_ind,
    lifespan_name,low_growing_grass_ind,nitrogen_fixation_potential,resprout_ability_ind,shape_orientation_name,toxicity_name,pd_max_range,
    ff_min_range,ph_min_range,ph_max_range,density_min_range,precip_min_range,precip_max_range,root_min_range,temp_min_range,
    commercial_availability_id,fruit_seed_abundance_id,propagated_by_bare_root_ind,propagated_by_bulb_ind,coarse_texture_soil_adaptable_ind,
    medium_texture_soil_adaptable_ind,fine_texture_soil_adaptable_ind,anaerobic_tolerance,caco3_tolerance,cold_stratification_required_ind,
    drought_tolerance,fire_tolerance,hedge_tolerance,moisture_usage,soil_ph_tolerance_min,soil_ph_tolerance_max,precipitation_tolerance_min,
    precipitation_tolerance_max,salinity_tolerance,shade_tolerance_name,temperature_tolerance_min,bloom_period,fruit_seed_period_start,
    fruit_seed_period_end,fruit_seed_persistence_ind,seed_per_pound,seed_spread_rate,seedling_vigor,vegetative_spread_rate,
    berry_nut_seed_product_ind,fodder_product_ind,palatability_browse,palatability_graze,palatability_human_ind,protein_potential,
    frost_free_days_min,planting_density_min,root_depth_min,vegetation,family
  `;

  if (state) {
    groupBy = `
      GROUP BY
      plant_symbol,plant_master_id,cultivar,full_scientific_name_without_author,primary_vernacular,plant_duration_name,
      ${regionRegex},
      plant_growth_habit_name,cover_crop,active_growth_period,after_harvest_regrowth_rate,bloat_potential,c_n_ratio,
      coppice_potential_ind,fall_conspicuous_ind,fire_resistant_ind,color_name,flower_conspicuous_ind,summer,winter,foliage_texture_name,
      fruit_seed_conspicuous_ind,growth_form_name,growth_rate,height_max_at_base_age,height_at_maturity,known_allelopath_ind,leaf_retention_ind,
      lifespan_name,low_growing_grass_ind,nitrogen_fixation_potential,resprout_ability_ind,shape_orientation_name,toxicity_name,pd_max_range,
      ff_min_range,ph_min_range,ph_max_range,density_min_range,precip_min_range,precip_max_range,root_min_range,temp_min_range,
      commercial_availability_id,fruit_seed_abundance_id,propagated_by_bare_root_ind,propagated_by_bulb_ind,coarse_texture_soil_adaptable_ind,
      medium_texture_soil_adaptable_ind,fine_texture_soil_adaptable_ind,anaerobic_tolerance,caco3_tolerance,cold_stratification_required_ind,
      drought_tolerance,fire_tolerance,hedge_tolerance,moisture_usage,soil_ph_tolerance_min,soil_ph_tolerance_max,precipitation_tolerance_min,
      precipitation_tolerance_max,salinity_tolerance,shade_tolerance_name,temperature_tolerance_min,bloom_period,fruit_seed_period_start,
      fruit_seed_period_end,fruit_seed_persistence_ind,seed_per_pound,seed_spread_rate,seedling_vigor,vegetative_spread_rate,
      berry_nut_seed_product_ind,fodder_product_ind,palatability_browse,palatability_graze,palatability_human_ind,protein_potential,
      frost_free_days_min,planting_density_min,root_depth_min,vegetation,family
    `;
  }

  // console.log(groupBy);

  const sq = querySymbols.length
    ? `
        SELECT DISTINCT ${columns} FROM plants3.characteristics
        WHERE plant_symbol IN (${querySymbols}) ${stateCond}
        ${groupBy}
      `
    : `
        SELECT DISTINCT ${columns} FROM plants3.characteristics
        WHERE active_growth_period IS NOT NULL ${stateCond}
        ${groupBy}
      `;

  // res.send(sq); return;

  console.time('query');
  const results = (await pool.query(sq)).rows;
  results.forEach((row) => {
    row.species = row.primary_vernacular || row.full_scientific_name_without_author;
    row.species = row.species[0].toUpperCase() + row.species.slice(1);
    row.scientific = row.full_scientific_name_without_author;
    row.key = `${row.plant_master_id}|${row.species}|${row.cultivar || ''}`;
  });
  console.timeEnd('query'); // 1s

  console.time('filter');
  let finalResults = results
    .sort((a, b) => a.plant_symbol.localeCompare(b.plant_symbol) || (a.cultivar || '').localeCompare(b.cultivar || ''))
    .filter((a, i, arr) => JSON.stringify(a) !== JSON.stringify(arr[i - 1]));

  if (symbols.length) {
    finalResults = finalResults.filter((a) => symbols.includes(a.plant_symbol));
  }

  // res.send(finalResults); return;

  // res.send({ allowedCultivars }); return;
  if (Object.keys(allowedCultivars).length) {
    finalResults = finalResults.filter((row) => {
      // console.log(row.cultivar, allowedCultivars[row.plant_symbol], allowedCultivars[row.plant_symbol]?.includes(row.cultivar));
      const result = !row.cultivar || allowedCultivars[row.plant_symbol]?.includes(row.cultivar);
      return result;
    });
  }

  // res.send(finalResults); return;

  stateData.forEach((row) => {
    if (symbols.includes(row.plant_symbol)) {
      let obj = finalResults.find((frow) => (
        (frow.plant_symbol === row.plant_symbol)
        && ((frow.cultivar || '') === (row.cultivar_name || ''))
      ));

      if (!obj) {
        obj = { ...finalResults[0] };
        Object.keys(obj).forEach((key) => { obj[key] = null; });
        finalResults.push(obj);
      }
      obj.plant_symbol = row.plant_symbol;
      obj.cultivar = row.cultivar_name;
      obj[row.parameter] = row.value;

      obj.plant_master_id = row.plant_master_id;

      const add = (parm) => {
        if (
          (state === 'AK' && row.plant_nativity_region_name === 'Alaska')
          || (state === 'HI' && row.plant_nativity_region_name === 'Hawaii')
          || (row.plant_nativity_region_name === 'Lower 48 States')
        ) {
          obj.plant_nativity_region_name = row.plant_nativity_region_name;
          if (row[parm] && !obj[parm]?.includes(row[parm])) {
            if (!obj[parm]) {
              obj[parm] = row[parm];
            } else {
              obj[parm] = ((obj[parm] || '').split(', ')) || [];
              obj[parm].push(row[parm]);
              obj[parm] = obj[parm].sort().join(', ');
            }
          }
        }
      }; // add

      if (row.plant_nativity_region_name === 'Lower 48 States') {
        obj.plant_nativity_region_name = row.plant_nativity_region_name;
        add('plant_duration_name');
        add('plant_nativity_type');
        add('plant_growth_habit_name');

        obj.cover_crop = row.cover_crop || false;
      }
    }
  });

  console.timeEnd('filter'); // 300ms

  finalResults = finalResults.sort((a, b) => (
    a.plant_symbol?.localeCompare(b.plant_symbol)
    || (a.cultivar || '').localeCompare(b.cultivar || '')
    || (b.mlra || '').localeCompare(a.mlra || '')
  ));

  // this may remove common from the editor!!!:
  // if (state) {
  //   finalResults = finalResults.filter((row) => row.mlra);
  // }

  if (state) {
    const done = {};

    finalResults = finalResults.filter((a) => a.full_scientific_name_without_author); // AGCR80

    finalResults = finalResults.filter((row1) => {
      // if (!stateData.find((row2) => (
      //   row1.plant_symbol === row2.plant_symbol
      //   && (row1.cultivar || '') === (row2.cultivar_name || '')
      // ))) {
      //   return false;
      // }

      const fs = row1.plant_symbol + (row1.cultivar || '');

      if (row1.mlra || !done[fs]) {
        done[fs] = true;
        return true;
      }

      return false;
    });
  }

  if (!finalResults.length) {
    res.send([]);
    return;
  }

  if (req.query.missing) {
    finalResults = finalResults.filter((row) => !row[req.query.missing]);
  }

  if (req.query.output === 'html') {
    finalResults.forEach((row) => {
      const p = row.plant_symbol;
      row.plant_symbol = `<a target="_blank" href="https://plants.sc.egov.usda.gov/plant-profile/${p}">${p}</a>`;
      return row;
    });
  }

  if (req.query.hideempty) {
    for (const key of Object.keys(finalResults[0])) {
      if (finalResults.every((row) => row[key] === null)) {
        for (const row of finalResults) delete row[key];
      }
    }
  }

  sendResults(req, res, finalResults);
}; // routeCharacteristics

const routeDeleteState = (req, res) => {
  simpleQuery(
    'DELETE FROM plants3.states WHERE state=$1',
    [req.query.state],
    req,
    res,
  );
}; // routeDeleteState

const routeRenameCultivar = async (req, res) => {
  try {
    await pool.query(
      `UPDATE plants3.states SET cultivar_name=$1 WHERE plant_symbol=$2 AND cultivar_name=$3;`,
      [req.query.newname, req.query.symbol, req.query.oldname],
    );

    [
      'plant_morphology_physiology',
      'plant_growth_requirements',
      'plant_reproduction',
      'plant_suitability_use',
    ].forEach((table) => {
      pool.query(
        `
          UPDATE plants3.${table} a
          SET cultivar_name=$1
          FROM plants3.plant_master_tbl b
          WHERE
            a.plant_master_id = b.plant_master_id
            AND b.plant_symbol = $2
            AND a.cultivar_name = $3;
        `,
        [req.query.newname, req.query.symbol, req.query.oldname],
      );
    });

    res.send({ status: 'Success' });
  } catch (error) {
    res.send({ error });
  }
}; // routeRenameCultivar

const routeSaveState = async (req, res) => {
  const {
    state, symbol, cultivar, parameter, value, note,
  } = req.query;

  console.log({
    state, symbol, cultivar, parameter, value, note,
  });
  // console.log(state, symbol, cultivar, parameter, value, notes);

  const symbols = symbol.split('|');
  const cultivars = (cultivar || '').split('|');
  const parameters = parameter.split('|');
  const values = value.split('|');
  const notes = (note || '').split('|');

  pool.query('DROP TABLE IF EXISTS plants3.characteristics');

  try {
    let i = 0;
    // eslint-disable-next-line no-restricted-syntax
    for (const sym of symbols) {
      // console.log({
      //   state, sym, cultivar: cultivars[i], parameter: parameters[i], value: values[i], note: notes[i],
      // });
      // eslint-disable-next-line no-await-in-loop
      await pool.query(
        'INSERT INTO plants3.states (state, plant_symbol, cultivar_name, parameter, value, notes) VALUES ($1, $2, $3, $4, $5, $6)',
        [state, sym || null, cultivars[i] || null, parameters[i] || null, values[i] || null, notes[i] || null],
      );
      i += 1;
    }
    sendResults(req, res, { status: 'Success' });
  } catch (error) {
    console.error(error);
    sendResults(req, res, { error });
  }
}; // routeSaveState

const routeState = async (req, res) => {
  simpleQuery(
    `
      SELECT * FROM plants3.states
      WHERE state=$1
      ORDER BY plant_symbol, cultivar_name, parameter
    `,
    [req.query.state],
    req,
    res,
  );
}; // routeState

const routeEditState = async (req, res) => {
  const {
    value, state, symbol, cultivar, parameter,
  } = req.query;

  pool.query(
    `
      UPDATE plants3.states
      SET value=$1
      WHERE
        state=$2
        AND plant_symbol=$3
        AND (
          cultivar_name=$4 OR (cultivar_name IS NULL AND $4 IS NULL)
        )
        AND parameter=$5
    `,
    [value, state, symbol, cultivar || null, parameter],
    (error, results) => {
      if (error) {
        console.log(error);
        debug({
          value, state, symbol, cultivar, parameter, error,
        }, req, res, 500);
      } else if (results.rowCount) {
        sendResults(req, res, { Success: `${results.rowCount} row updated` });
      } else {
        pool.query(
          `
            INSERT INTO plants3.states
            (value, state, plant_symbol, cultivar_name, parameter)
            VALUES ($1, $2, $3, $4, $5)
          `,
          [value, state, symbol, cultivar || null, parameter],
          (error2, results2) => {
            if (error2) {
              console.log(error2);
              debug({
                value, state, symbol, cultivar, parameter, error,
              }, req, res, 500);
            } else {
              sendResults(req, res, { Success: `${results2.rowCount} row inserted` });
            }
          },
        );
      }
    },
  );
}; // routeEditState

const routeProps = async (req, res) => {
  /* eslint-disable max-len */
  const results = await pool.query(`
    SELECT 'coppice_potential_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'fall_conspicuous_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'fire_resistant_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'flower_conspicuous_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'fruit_seed_conspicuous_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'known_allelopath_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'leaf_retention_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'low_growing_grass_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'resprout_ability_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'propagated_by_bare_root_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'propagated_by_bulb_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'coarse_texture_soil_adaptable_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'medium_texture_soil_adaptable_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'fine_texture_soil_adaptable_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'cold_stratification_required_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'fruit_seed_persistence_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'berry_nut_seed_product_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'fodder_product_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    SELECT 'palatability_human_ind' AS parm, ARRAY[null, 'true', 'false'] AS array_agg UNION ALL
    
    SELECT 'plant_duration_name' AS parm, ARRAY_AGG(DISTINCT plant_duration_name ORDER BY plant_duration_name NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'plant_nativity_type' AS parm, ARRAY_AGG(DISTINCT plant_nativity_type ORDER BY plant_nativity_type NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'plant_nativity_region_name' AS parm, ARRAY_AGG(DISTINCT plant_nativity_region_name ORDER BY plant_nativity_region_name NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'plant_growth_habit_name' AS parm, ARRAY_AGG(DISTINCT plant_growth_habit_name ORDER BY plant_growth_habit_name NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'active_growth_period' AS parm, ARRAY_AGG(DISTINCT active_growth_period ORDER BY active_growth_period NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'after_harvest_regrowth_rate' AS parm, ARRAY_AGG(DISTINCT after_harvest_regrowth_rate ORDER BY after_harvest_regrowth_rate NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'bloat_potential' AS parm, ARRAY_AGG(DISTINCT bloat_potential ORDER BY bloat_potential NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'c_n_ratio' AS parm, ARRAY_AGG(DISTINCT c_n_ratio ORDER BY c_n_ratio NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'color_name' AS parm, ARRAY_AGG(DISTINCT color_name ORDER BY color_name NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'summer' AS parm, ARRAY_AGG(DISTINCT summer ORDER BY summer NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'winter' AS parm, ARRAY_AGG(DISTINCT winter ORDER BY winter NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'foliage_texture_name' AS parm, ARRAY_AGG(DISTINCT foliage_texture_name ORDER BY foliage_texture_name NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'growth_form_name' AS parm, ARRAY_AGG(DISTINCT growth_form_name ORDER BY growth_form_name NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'growth_rate' AS parm, ARRAY_AGG(DISTINCT growth_rate ORDER BY growth_rate NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'lifespan_name' AS parm, ARRAY_AGG(DISTINCT lifespan_name ORDER BY lifespan_name NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'nitrogen_fixation_potential' AS parm, ARRAY_AGG(DISTINCT nitrogen_fixation_potential ORDER BY nitrogen_fixation_potential NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'shape_orientation_name' AS parm, ARRAY_AGG(DISTINCT shape_orientation_name ORDER BY shape_orientation_name NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'toxicity_name' AS parm, ARRAY_AGG(DISTINCT toxicity_name ORDER BY toxicity_name NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'pd_max_range' AS parm, ARRAY_AGG(DISTINCT pd_max_range ORDER BY pd_max_range NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'ff_min_range' AS parm, ARRAY_AGG(DISTINCT ff_min_range ORDER BY ff_min_range NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'ph_min_range' AS parm, ARRAY_AGG(DISTINCT ph_min_range ORDER BY ph_min_range NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'ph_max_range' AS parm, ARRAY_AGG(DISTINCT ph_max_range ORDER BY ph_max_range NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'density_min_range' AS parm, ARRAY_AGG(DISTINCT density_min_range ORDER BY density_min_range NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'precip_min_range' AS parm, ARRAY_AGG(DISTINCT precip_min_range ORDER BY precip_min_range NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'precip_max_range' AS parm, ARRAY_AGG(DISTINCT precip_max_range ORDER BY precip_max_range NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'root_min_range' AS parm, ARRAY_AGG(DISTINCT root_min_range ORDER BY root_min_range NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'temp_min_range' AS parm, ARRAY_AGG(DISTINCT temp_min_range ORDER BY temp_min_range NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'anaerobic_tolerance' AS parm, ARRAY_AGG(DISTINCT anaerobic_tolerance ORDER BY anaerobic_tolerance NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'caco3_tolerance' AS parm, ARRAY_AGG(DISTINCT caco3_tolerance ORDER BY caco3_tolerance NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'drought_tolerance' AS parm, ARRAY_AGG(DISTINCT drought_tolerance ORDER BY drought_tolerance NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'fire_tolerance' AS parm, ARRAY_AGG(DISTINCT fire_tolerance ORDER BY fire_tolerance NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'hedge_tolerance' AS parm, ARRAY_AGG(DISTINCT hedge_tolerance ORDER BY hedge_tolerance NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'moisture_usage' AS parm, ARRAY_AGG(DISTINCT moisture_usage ORDER BY moisture_usage NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'salinity_tolerance' AS parm, ARRAY_AGG(DISTINCT salinity_tolerance ORDER BY salinity_tolerance NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'shade_tolerance_name' AS parm, ARRAY_AGG(DISTINCT shade_tolerance_name ORDER BY shade_tolerance_name NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'bloom_period' AS parm, ARRAY_AGG(DISTINCT bloom_period ORDER BY bloom_period NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'fruit_seed_period_start' AS parm, ARRAY_AGG(DISTINCT fruit_seed_period_start ORDER BY fruit_seed_period_start NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'fruit_seed_period_end' AS parm, ARRAY_AGG(DISTINCT fruit_seed_period_end ORDER BY fruit_seed_period_end NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'seed_spread_rate' AS parm, ARRAY_AGG(DISTINCT seed_spread_rate ORDER BY seed_spread_rate NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'seedling_vigor' AS parm, ARRAY_AGG(DISTINCT seedling_vigor ORDER BY seedling_vigor NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'vegetative_spread_rate' AS parm, ARRAY_AGG(DISTINCT vegetative_spread_rate ORDER BY vegetative_spread_rate NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'palatability_browse' AS parm, ARRAY_AGG(DISTINCT palatability_browse ORDER BY palatability_browse NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'palatability_graze' AS parm, ARRAY_AGG(DISTINCT palatability_graze ORDER BY palatability_graze NULLS FIRST) FROM plants3.characteristics UNION ALL
    SELECT 'protein_potential' AS parm, ARRAY_AGG(DISTINCT protein_potential ORDER BY protein_potential NULLS FIRST) FROM plants3.characteristics
  `);
  /* eslint-enable max-len */

  const obj = {};
  results.rows.forEach((row) => {
    obj[row.parm] = row.array_agg;
  });

  const statesResults = await pool.query(`SELECT DISTINCT parameter, value FROM plants3.states WHERE parameter NOT IN ('cps', 'mlra')`);
  statesResults.rows.forEach((row) => {
    if (!obj[row.parameter]?.includes(row.value)) {
      obj[row.parameter] = obj[row.parameter] || [];
      obj[row.parameter].push(row.value);
    }
  });
  sendResults(req, res, obj);
}; // routeProps

const routeSymbols = async (req, res) => {
  let results;

  if (req.query.state) {
    results = await pool.query(
      `SELECT DISTINCT plant_symbol FROM plants3.states WHERE state = $1 ORDER BY 1`,
      [req.query.state.toUpperCase()],
    );
  } else {
    results = await pool.query('SELECT TRIM(plant_symbol) AS plant_symbol FROM plants3.plant_master_tbl');
  }

  sendResults(req, res, results.rows.map((row) => row.plant_symbol));
}; // routeSymbols

const routeNewSpecies = async (req, res) => {
  const { state, symbol, cultivar } = req.query;
  pool.query(
    'SELECT * FROM plants3.states WHERE state=$1 AND plant_symbol=$2 AND cultivar_name=$3',
    [state, symbol, cultivar],
    (err, results) => {
      if (err) {
        debug(err, req, res, 500);
      } else if (results.rows.length) {
        res.send({ status: 'Species already exists' });
      } else {
        pool.query(
          'INSERT INTO plants3.states (state, plant_symbol, cultivar_name) values ($1, $2, $3)',
          [state, symbol, cultivar],
          (err2) => {
            if (err2) {
              debug(err, req, res, 500);
            } else {
              res.send({ status: 'Success' });
            }
          },
        );
      }
    },
  );
}; // routeNewSpecies

const routeRecords = (req, res) => {
  // https://stackoverflow.com/a/38684225/3903374 and Chat-GPT
  const sq = `
    SELECT
      table_name AS "table",
      (SELECT reltuples::bigint FROM pg_class WHERE relname = ts.table_name) AS "rows",
      pg_total_relation_size(format('%I.%I', table_schema, table_name)) AS size,
      pg_size_pretty(pg_total_relation_size(format('%I.%I', table_schema, table_name))) AS prettysize
    FROM information_schema.tables ts
    WHERE table_schema = 'plants3'
    ORDER BY table_name;
  `;

  simpleQuery(sq, [], req, res);
}; // routeRecords

const routeStructure = (req, res) => {
  const { table } = req.query;

  const sq = `
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE ${table ? `table_name = $1 AND ` : ''}
      table_schema = 'plants3'
    ORDER BY table_name, ordinal_position;
  `;

  simpleQuery(sq, table ? [table] : [], req, res);
}; // routeStructure

const routePlantsEmptyColumns = async (req, res) => {
  if (!req.query.generate) {
    const empty = {
      // eslint-disable-next-line max-len
      plant_conservation_status_qualifier: [], plant_image_library: ['plant_image_library_id', 'plant_image_id', 'stream_id', 'last_change_date', 'last_changed_by', 'creation_date', 'created_by', 'active_record_ind'], plant_reserved_symbol_tbl: ['plant_family', 'plant_family_symbol', 'plant_family_id', 'subvariety', 'bauthor_data_source_id', 'tauthor_data_source_id', 'qauthor_data_source_id', 'ssauthor_data_source_id', 'fauthor_data_source_id', 'plant_category', 'plant_category_id', 'hybrid_parent', 'hybrid_parent1', 'hybrid_parent2', 'hybrid_parent3', 'suffix', 'svauthor', 'svauthor_id'], plant_conservation_status: [], county_gen2_project_webmercator: ['objectid', 'shape', 'name', 'state_name', 'fips', 'st'], generated_symbols_with_authorship: [], plant_noxious_status: [], d_plant_location_reference_subject: ['plant_location_reference_subject_description'], dw_plant_images: ['plant_images_id', 'plant_symbol', 'plant_master_id', 'parent_master_id', 'plant_rank', 'plant_synonym_ind', 'plant_full_scientific_name', 'plant_full_scientific_name_without_author', 'plant_scientific_name_html', 'plant_sciname_sort', 'plant_family', 'plant_family_symbol', 'plant_primary_vernacular', 'plant_image_type', 'plant_image_purpose', 'provided_by', 'provided_by_sortname', 'scanned_by', 'scanned_by_sortname', 'originally_from', 'originally_from_sortname', 'author', 'author_sortname', 'contributorindividual', 'contrib_ind_sortname', 'contributororganization', 'contrib_org_sortname', 'plantauthorship', 'plant_author_sortname', 'artist', 'artist_sortname', 'copyrightholder', 'copyright_sortname', 'other', 'other_sortname', 'plant_reference_title', 'plant_reference_place', 'plant_reference_year', 'plant_publication_volume_nbr', 'plant_publication_issue', 'plant_reference_publication', 'plant_reference_media_type', 'plant_reference_source_type', 'plant_institution_name', 'plant_image_website_url', 'plant_source_email', 'plant_imagelocation', 'plant_imagecreationdate', 'plant_copyright_ind', 'plant_image_country_fullname', 'plant_image_country_abbr', 'plant_image_state', 'plant_image_state_abbr', 'plant_image_county', 'plant_image_city', 'plant_image_locality', 'plant_image_fips', 'plant_image_geoid', 'plant_image_notes', 'plant_image_primary_ind', 'plant_image_display_ind', 'plant_image_id', 'plant_country_identifier', 'plant_location_id', 'plant_country_subdivision_id', 'plant_reference_id', 'plant_image_last_updated', 'plant_location_last_updated', 'plant_image_cred_last_updated', 'plant_reference_last_updated', 'dw_record_updated'], d_lifespan: [], plant_duration: [], plant_global_conservation: [], plant_hybrid_formula: [], d_common_name_type: [], d_country: [], d_plant_image_purpose: ['plant_image_purpose_description'], d_plant_wetland: [], staging_plant_invasive: ['staging_plant_invasive_id', 'plant_symbol', 'plant_synonym', 'accepted_sciname', 'useifdifferent_sci', 'common_name', 'state_status', 'plant_master_id', 'plant_syn_id', 'common_name_id', 'state_status_id', 'location_abbr', 'location_code', 'location_name', 'creation_date', 'created_by', 'processed'], d_plant_wildlife_food: [], plant_protected_status: [], plant_reference: ['plant_reference_acronym', 'state_county_code', 'secondary_reference_title', 'reference_hyperlink'], d_plant_pollinator: [], plant_usage: ['plant_usage_id', 'plant_use_id', 'plant_location_characteristic_id', 'active_record_ind', 'creation_date', 'created_by', 'last_change_date', 'last_changed_by'], d_noxious_status: [], plant_notes: ['synonym_notes', 'subordinate_taxa_notes', 'legal_notes', 'noxious_notes', 'rarity_notes', 'wetland_notes', 'related_links_notes', 'wildlife_notes', 'sources_notes', 'characteristic_notes', 'pollinator_notes', 'cultural_notes', 'ethnobotany_notes'], d_protected_status_source: [], plant_data_source: ['plant_data_source_last_name', 'plant_data_source_first_name', 'plant_data_source_website_url'], d_plant_reference_purpose: [], d_noxious_status_source: [], audit_plant_master_tbl: ['plant_master_update_id', 'action_taken', 'plant_master_id', 'plant_hierarchy_id', 'plant_symbol', 'plant_status_id', 'plant_rank_id', 'plant_synonym_ind', 'plant_scientific_name', 'plant_author_name_id', 'plant_primary_vernacular_id', 'plant_revisor_id', 'full_scientific_name', 'full_scientific_name_html', 'full_scientific_name_without_author', 'is_active', 'parent_master_id', 'is_taxa', 'taxa_master_id', 'gsat', 'cover_crop', 'cultural_significant_ind', 'action_date', 'action_taken_by', 'action_generated_from'], audit_invasive_source: [], d_plant_wildlife_type: [], d_invasive_status_source: [], staging_plant_wetland: ['parent_region_id'], d_plant_wildlife_cover: [], d_plant_status: ['last_change_date', 'last_changed_by'], d_country_subdivision_type: [], entitlement: [], plant_ethnobotany: [], audit_plant_reference: ['plant_reference_acronym', 'plant_reference_second_title', 'plant_reference_place', 'state_county_code', 'plant_publication_volume_nbr', 'plant_website_url_text', 'plant_author', 'plant_publisher'], d_country_complete: ['end_date'], d_plant_occurrence_type: ['plant_occurrence_type_description'], plant_synonym_tbl: [], d_color: [], d_plant_use: ['plant_use_description', 'last_changed_by'], plant_wildlife: [], plant_data_sources: [], audit_plant_ref_association: ['plant_ref_assoc_id', 'plant_master_id', 'plant_literature_id', 'plant_reference_id', 'action_taken', 'action_date', 'is_active'], plant_image: ['plant_image_file_name', 'plant_image', 'plant_image_notes', 'plant_image_location_latitude', 'plant_image_location_longitude'], plant_unknown_tbl: [], plant_reference_source: [], plant_invasive_status: [], d_foliage_porosity: [], plant_region: [], plant_suitability_use: [], d_plant_nativity_region: [], plant_image_credit: [], plant_growth_requirements: [], d_plant_ethno_culture: ['plant_ethno_culture_notes'], plant_occurrence_location: [], plant_location_characteristic: ['plant_noxious_status_id'], audit_plant_location_common_name: ['plant_location_common_name_audit_id', 'action_taken', 'plant_master_id', 'plant_location_id', 'plant_primary_vernacular_id', 'action_taken_from', 'is_active', 'action_date', 'action_taken_by'], audit_plant_data_source: ['plant_data_source_email_address', 'contributor_id', 'plant_data_source_website_url'], linegeometries: ['id', 'shape', 'code'], d_foliage_texture: [], d_plant_nativity: [], plant_classifications_tbl: ['suborder', 'subfamily', 'classid_hybrid_author', 'taxquest'], d_plant_name_suffix: ['plant_name_suffix_description'], document_delete: ['Word Files', 'PDF files'], d_crop_type: [], audit_plant_synonym_tbl: ['plant_synonym_update_id', 'action_taken', 'plant_synonym_id', 'plant_master_id', 'synonym_plant_master_id', 'is_active', 'action_date', 'action_taken_by', 'action_generated_from'], plant_data_source_detail: ['plant_data_source_address', 'plant_data_source_city', 'plant_data_source_state', 'plant_data_source_phone', 'plant_data_source_affiliations'], d_extent: [], d_plant_taxonomic_status: ['last_changed_by'], d_shape_orientation: [], d_plant_ethno_use: ['plant_ethno_usage_definition'], '8ball_data': [], plant_reproduction: [], role_entitlement: [], plant_pollinator: [], d_plant_image_credit_type: [], staging_symbol_generator: ['reservedfor_id', 'formauthorid', 'varietyauthorid', 'subvarietyauthorid', 'subspeciesauthorid', 'speciesauthorid', 'genusauthorid', 'accepted_symbol', 'acceptedid'], d_plant_record_type: ['last_change_date', 'last_changed_by'], staging_plant_invasive_source: ['staging_plant_invasive_source_id', 'author', 'inv_year', 'hyperlink_txt', 'inv_url', 'location_abbr', 'location_code', 'location_name', 'creation_date', 'created_by', 'processed'], plant_master_image: ['plant_image_purpose_id'], plant_location_reference: ['plant_location_reference_id', 'plant_location_characteristic_id', 'plant_reference_id', 'plant_location_subject_id', 'plant_reference_purpose_id', 'creation_date', 'created_by', 'last_change_date', 'last_changed_by', 'active_record_ind'], alternative_crop: [], d_plant_action: [], d_rate: [], audit_plant_image: ['plant_image_audit_id', 'plant_master_id', 'plant_image_id', 'plant_reference_id', 'plant_image_type_id', 'plant_image_taken_date', 'plant_image_primary_ind', 'plant_image_display_ind', 'plant_image_copyrighted_ind', 'plant_image_stream_id', 'plant_image_location', 'plant_image_notes', 'action_taken', 'action_date', 'action_taken_by', 'active_record_ind'], plant_vascular: ['taxa_master_id'], d_plant_vernacular: [], state_gen_nonus_project_webmercator: ['objectid', 'shape', 'state_name', 'identifier'], audit_plant_invasive_status: ['plant_invasive_id'], d_plant_wetland_region: [], audit_plant_master_image: ['plant_master_image_audit_id', 'plant_master_id', 'plant_master_image_id', 'plant_image_purpose_id', 'action_taken', 'action_date', 'action_taken_by', 'active_record_ind'], audit_plant_work_basket: ['audit_work_basket_id', 'plant_work_basket_id', 'table_name', 'table_record_id', 'process_status_id', 'notes', 'action_date', 'action_taken_by'], plant_herbarium_image: ['plant_herbarium_image_id', 'plant_herbarium_id', 'plant_image_id', 'active_record_ind', 'creation_date', 'created_by', 'last_change_date', 'last_changed_by'], plant_cultural: [], d_plant_family_category: [], d_growth_form: [], audit_plant_wetland: ['plant_wetland_notes'], d_protected_status: [], audit_plant_image_credit: ['plant_image_credit_audit_id', 'plant_master_id', 'plant_image_credit_id', 'plant_image_id', 'plant_image_prefix_id', 'plant_image_credit_type_id', 'plant_image_data_source_id', 'plant_image_credit_display_ind', 'action_taken', 'action_date', 'action_taken_by', 'active_record_ind'], d_season: [], plants_work_basket: ['plant_work_basket_id', 'table_name', 'table_record_id', 'process_status_id', 'notes', 'creation_date', 'created_by', 'last_change_date', 'last_changed_by'], plant_location_common_name: [], plant_occurrence: ['plant_collection_nbr', 'plant_location_description', 'plant_specific_location_description', 'plant_habitat_description', 'plant_determination_date'], plant_data_reference: ['plant_publication_chapter'], audit_plant_image_library: ['plant_image_library_audit_id', 'plant_image_library_id', 'plant_image_id', 'plant_image_name', 'plant_image_new_name', 'plant_stream_id', 'action_taken', 'action_date', 'action_taken_by', 'active_record_ind'], d_plant_website_type: [], plant_common_name: ['last_change_date', 'last_changed_by'], d_commercial_availability: [], plant_spotlight: ['last_change_date', 'last_changed_by'], dw_plant_wetland: ['plant_dw_wetland_id', 'plant_wetland_symbol', 'plant_accepted_symbol', 'plant_master_id', 'parent_master_id', 'plant_rank', 'plant_synonym_ind', 'plant_scientific_name', 'plant_full_scientific_name', 'plant_full_scientific_name_without_author', 'plant_scientific_name_html', 'plant_sciname_sort', 'plant_family', 'plant_family_symbol', 'plant_primary_vernacular', 'plant_region', 'plant_subregion', 'plant_region_description', 'plant_region_abbreviation', 'plant_parent_region_abbreviation', 'plant_parent_region_description', 'plant_wetland_notes', 'plant_wetland_status_abbreviation', 'plant_wetland_status_description', 'plant_wetland_status_name', 'plant_hydrophyte_ind', 'plant_location_id', 'plant_location_characteristic_id', 'plant_wetland_status_id1', 'plant_wetland_region_id', 'plant_wetland_parent_id', 'plant_region_last_updated', 'plant_base_data_last_updated', 'plant_wetland_status_last_updated', 'plant_dw_record_last_updated'], plant_literature_location: [], d_plant_rank: ['display_sequence', 'last_change_date', 'last_changed_by'], alternative_crop_information: [], d_plant_image_type: [], plant_master_tbl: ['taxa_master_id'], role: [], d_plant_herbarium: ['plant_reference_id'], d_plant_duration: [], d_country_subdivision_category: [], d_country_subdivision: ['country_subdivision_level'], plant_location: ['state_county_code', 'plant_location_shape'], plant_ethnobotany_source: [], d_plant_reserved_status: [], staging_plant_wetland_import: ['plant_wetland_import_id', 'plant_scientific_name', 'plant_symbol', 'plant_synonym', 'wetland_symbol', 'hi', 'cb', 'ak', 'aw', 'agcp', 'emp', 'gp', 'mw', 'ncne', 'wmvc', 'aki', 'acp', 'cil', 'crb', 'iah', 'ial', 'iam', 'ngl', 'nbr', 'nsl', 'pda', 'sph', 'spi', 'ukk', 'wbrmnt', 'wgc', 'creation_date', 'created_by'], odmt_authorized: ['odmt_role1', 'odmt_role2', 'odmt_role3', 'last_changed_by'], d_plant_reference_type: [], plant_literature: [], d_plant_reserved_for: [], plants_document_remove: ['plants_doc_remove_id', 'plant_document_name', 'creation_date', 'created_by', 'last_change_date', 'last_changed_by', 'active_record_ind'], audit_plant_notes: ['synonym_notes', 'subordinate_taxa_notes', 'legal_notes', 'noxious_notes', 'rarity_notes', 'wetland_notes', 'related_links', 'wildlife_notes', 'sources_notes', 'characteristic_notes', 'pollinator_notes', 'cultural_notes', 'ethnobotany_notes'], d_plant_family: ['plant_family_alt_sym'], gsat_lkup: [], plant_related_website: ['plant_website_url_suffix'], d_conservation_status_rank: [], d_plant_growth_habit: [], d_state_county: ['coastal_county_ind', 'countyseat_geometry', 'state_county_geometry'], dw_plant_master_profile: ['plant_master_profile_id', 'plant_master_id', 'plant_symbol', 'plant_rank', 'plant_rank_id', 'plant_synonym_ind', 'plant_is_hybrid_ind', 'plant_full_scientific_name', 'plant_full_scientific_name_without_author', 'plant_scientific_name_html', 'plant_sciname_sort', 'plant_author', 'plant_author_id', 'plant_revisor', 'plant_revisor_id', 'plant_primary_vernacular', 'plant_primary_vernacular_id', 'plant_state_vernacular', 'plant_vernacular_state', 'plant_vernacular_trademark', 'plant_other_common_names', 'plant_group', 'plant_category', 'plant_family', 'plant_family_symbol', 'plant_family_vernacular', 'plant_noxious_ind', 'plant_global_rarity_ind', 'plant_us_rarity_ind', 'plant_wetland_ind', 'plant_invasive_ind', 'plant_vascular_ind', 'plant_duration1', 'plant_duration2', 'plant_duration3', 'plant_growth1', 'plant_growth2', 'plant_growth3', 'plant_growth4', 'plant_nat_l48', 'plant_nat_ak', 'plant_nat_hi', 'plant_nat_pr', 'plant_nat_vi', 'plant_nat_nav', 'plant_nat_can', 'plant_nat_gl', 'plant_nat_spm', 'plant_nat_na', 'plant_nat_pb', 'plant_nat_pfa', 'plantguide_pdf', 'plantguide_docx', 'factsheet_pdf', 'factsheet_docx', 'plant_master_notes', 'plant_synonym_notes', 'plant_subordinate_taxa_notes', 'plant_legal_notes', 'plant_taxonomic_status_suffix', 'gsat', 'cover_crop', 'cultural_significant_ind', 'is_taxa', 'taxa_master_id', 'parent_master_id', 'plant_hierarchy_id', 'plant_parent_hierarchy_id', 'plant_taxa_hierarchy_id', 'plant_hybrid_parent1', 'plant_hybrid_parent2', 'plant_hierarchy_level', 'plant_kingdom', 'plant_subkingdom', 'plant_superdivision', 'plant_division', 'plant_subdivision', 'plant_class', 'plant_order', 'plant_suborder', 'plant_subfamily', 'plant_xgenus', 'plant_genus', 'plant_xspecies', 'plant_species', 'plant_ssp', 'plant_xsubsp', 'plant_subspecies', 'plant_var', 'plant_xvariety', 'plant_variety', 'plant_subvariety', 'plant_f', 'plant_forma', 'bauthor', 'tauthor', 'qauthor', 'nomenclature', 'unaccept_reason', 'plant_base_data_last_updated', 'plant_classification_data_last_updated', 'dw_record_updated'], plant_growth_habit: [], plant_document_audit: [], state_nrcs_download: [], plant_master_document: [], d_toxicity: [], d_plant_data_source_type: ['plant_data_source_type_description'], d_plant_noxious_status: ['plant_noxious_status_name'], d_conservation_status_qualifier: [], d_shade_tolerance: [], state_gen_us_project_webmercator: ['objectid', 'shape', 'state_name', 'identifier'], d_process_status: ['process_status_id', 'process_status_name', 'process_status_definition', 'creation_date', 'created_by', 'last_change_date', 'last_changed_by', 'active_record_ind'], audit_plant_ref_source: [], audit_noxious_source: ['source_audit_id', 'action_taken', 'noxious_status_source_id', 'plant_location_id', 'noxious_status_sourc_text', 'is_active', 'action_date', 'action_taken_by'], plant_family_category: [], d_plant_image_prefix: [], plant_name_suffix: [], d_invasive_status: [], plant_morphology_physiology: ['hmaba_id', 'hmaba_display', 'ham_id', 'ham_display'],
    };

    sendResults(req, res, empty);
    return;
  }

  let tables = req.query.table ? [req.query.table] : [];

  if (!tables.length) {
    const results = await pool.query(`
      SELECT DISTINCT table_name
      FROM information_schema.columns
      WHERE table_schema = 'plants3';
    `);

    tables = results.rows.map((row) => row.table_name);
  }

  const empty = {};

  // eslint-disable-next-line no-restricted-syntax
  for await (const table of tables) {
    empty[table] = [];
    const results = await pool.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = '${table}';
    `);

    const columns = results.rows.map((row) => row.column_name);
    // eslint-disable-next-line no-restricted-syntax
    for await (const col of columns) {
      const result = await pool.query(`
        SELECT 1
        FROM plants3."${table}" as p
        WHERE p."${col}" IS NOT NULL
        limit 1
      `);

      if (!result.rows.length) {
        empty[table].push(col);
      }
    }
  }

  sendResults(req, res, empty);
}; // routePlantsEmptyColumns

const routePlantsTable = (req, res) => {
  const table = safeQuery(req, 'table');
  const sq = `SELECT * FROM plants3.${table} ORDER BY 1`;

  simpleQuery(sq, [], req, res, true);
}; // routePlantsTable

const routeMissingCultivars = async (req, res) => {
  const { state } = req.query;

  const results = await pool.query(`
    SELECT DISTINCT * FROM (
      SELECT
        COALESCE(a.plant_symbol, b.plant_symbol) AS symbol,
        b.cultivar_name AS cultivar,
        cultivars AS "known cultivars", state
      FROM (
        SELECT a.*, b.plant_symbol
        FROM (
          SELECT plant_master_id, ARRAY_AGG(cultivar_name ORDER BY cultivar_name) AS cultivars
          FROM plants3.plant_growth_requirements
          GROUP BY plant_master_id
        ) a
        JOIN plants3.plant_master_tbl b
        USING (plant_master_id)
        WHERE plant_symbol IN (
          SELECT plant_symbol FROM plants3.states
          ${state ? ' WHERE state = $1' : ''}
        )
      ) a
      FULL OUTER JOIN (
        SELECT COALESCE(cultivar_name, '') AS cultivar_name, plant_symbol, state
        FROM plants3.states
        ${state ? ' WHERE state = $1' : ''}
      ) b
      ON a.plant_symbol = b.plant_symbol
      ORDER BY a.plant_symbol
    ) a
    WHERE
      not cultivar = ANY("known cultivars") OR
      (cultivar > '' AND "known cultivars" IS NULL)
    ORDER BY state, symbol, cultivar
  `, state ? [state] : undefined);

  sendResults(
    req,
    res,
    results.rows.map((row) => {
      row.cultivar = row.cultivar || `<em style="color: gray">${row.cultivar || 'common'}</em>`;
      row.symbol = `<a target="_blank" href="https://plants.sc.egov.usda.gov/home/plantProfile?symbol=${row.symbol}">${row.symbol}</a>`;
      row['known cultivars'] = row['known cultivars']?.filter((c) => c).join(', ');
      return row;
    }),
    { rowspan: true },
  );
}; // routeMissingCultivars

const routeMoveCultivar = async (req, res) => {
  const {
    cultivar, from, to, plants,
  } = req.query;

  const schema = plants ? '' : 'plants3.';

  let query = '';

  if (!plants) {
    query += `
      DROP TABLE IF EXISTS plants3.characteristics;
      ----------------------------------------------
      
      UPDATE plants3.states
      SET plant_symbol = '${to}'
      WHERE plant_symbol = '${from}' AND cultivar_name = '${cultivar}';
      ----------------------------------------------
    `.replace(/ {6}/g, '');
  }

  [
    'plant_morphology_physiology',
    'plant_growth_requirements',
    'plant_reproduction',
    'plant_suitability_use',
  ].forEach((table) => {
    query += `
      UPDATE ${schema}${table} a
      SET plant_master_id = (
        SELECT plant_master_id
        FROM ${schema}plant_master_tbl
        WHERE plant_symbol = '${to}'
      )
      FROM ${schema}plant_master_tbl b
      WHERE
        a.plant_master_id = b.plant_master_id
        AND b.plant_symbol = '${from}'
        AND a.cultivar_name = '${cultivar}';
      ----------------------------------------------
    `.replace(/ {6}/g, '');
  });

  res.type('text/plain');
  res.send(query);
}; // routeMoveCultivar

const routeDatabaseChanges = async (req, res) => {
  const fetchData = async (query) => {
    const result = await pool.query(query);
    const id = result.fields[0].name;
    return {
      id,
      data: result.rows.reduce((acc, row) => {
        acc[row[id]] = row;
        return acc;
      }, {}),
    };
  };

  const symbols = (await pool.query('SELECT plant_master_id, plant_symbol FROM plants3.plant_master_tbl'))
    .rows.reduce((acc, row) => {
      acc[row.plant_master_id] = row.plant_symbol;
      return acc;
    }, {});

  // Number.isNaN doesn't try to coerce to numeric like isNaN does.
  // eslint-disable-next-line no-restricted-globals
  const format = (s) => (isNaN(s) ? `'${s}'` : s);

  let output = `
    <style>
      body, table {
        font: 13px arial;
      }
         
      table {
        border: 1px solid black;
        border-spacing: 0; 
        empty-cells: show;
      }
      
      td, th {
        padding: 0.2em 0.5em;
        border-right: 1px solid #ddd;
        border-bottom: 1px solid #bbb;
      }

      th {
        background: #def;
      }

      section {
        display: inline-block;
      }
    </style>
  `;

  for (const table of ['plant_growth_requirements', 'plant_morphology_physiology', 'plant_reproduction', 'plant_suitability_use']) {
    const rows = [];
    let id;
    let data;
    ({ id, data } = await fetchData(`SELECT * FROM plants3.${table}_backup ORDER BY 1`));
    const data1 = data;
    ({ id, data } = await fetchData(`SELECT * FROM plants3.${table} ORDER BY 1`));
    const data2 = data;

    output += `
      <section>
        <h3>${table}</h3>
        <table>
          <tr><th>${id}<th>plant_master_id<th>Cultivar<th>Column<th>Original<th>New<th>SQL
    `;
    for (const pid in data1) {
      if (Object.prototype.hasOwnProperty.call(data1, pid) && Object.prototype.hasOwnProperty.call(data2, pid)) {
        const keys = Object.keys(data1[pid]);
        for (const key of keys) {
          const value = (s, k) => (
            k === 'plant_master_id' ? `${s} <small>(${symbols[s]})</small>` : s?.toString()
          );

          const v1 = data1[pid][key]?.toString();
          const v2 = data2[pid][key]?.toString();
          if ((v1 || v2) && v1 !== v2) {
            rows.push([
              pid,
              value(data1[pid].plant_master_id, 'plant_master_id'),
              data1[pid].cultivar_name,
              key,
              value(data1[pid][key], key),
              value(data2[pid][key], key),
              `UPDATE ${table} SET ${key} = ${format(v2)} WHERE ${id} = ${pid};`,
            ]);
          }
        }
      }
    }

    output += rows
      .sort((a, b) => a[3].localeCompare(b[3]))
      .map((row) => `<tr><td>${row.join('<td>')}`)
      .join('');

    output += `
        </table>
      </section>
    `;
  }
  res.send(output);
}; // routeDatabaseChanges

const routeRetention = async (req, res) => {
  const sql = `
    SELECT DISTINCT
      CONCAT('<a target="_blank" href="https://plants.sc.egov.usda.gov/home/plantProfile?symbol=', plant_symbol, '">', plant_symbol, '</a>')
      AS symbol,
      full_scientific_name_without_author, plant_vernacular_name, leaf_retention_ind, vegetation
    FROM plants3.plant_morphology_physiology
    INNER JOIN plants3.plant_master_tbl
    USING (plant_master_id)
    LEFT JOIN plants3.vegetation
    USING (plant_symbol)
    LEFT JOIN plants3.d_plant_vernacular
    ON plant_primary_vernacular_id=plant_vernacular_id
    LEFT JOIN plants3.states
    USING (plant_symbol)
    WHERE
      (
        (vegetation ILIKE '%Decid%' AND leaf_retention_ind)
        OR (vegetation ILIKE '%Ever%' AND NOT leaf_retention_ind)
      ) ${req.query.state ? `AND state = '${req.query.state.slice(0, 2)}'` : ''}
    ORDER BY 4, 1;
  `;

  simpleQuery(sql, null, req, res);
};

const routeValidStates = async (req, res) => {
  const sql = `
    SELECT DISTINCT state
    FROM plants3.states
    WHERE state <> 'all'
    ORDER BY state;
  `;

  pool.query(
    sql,
    (err, results) => {
      sendResults(req, res, results.rows.map((row) => row.state));
    },
  );
};

const routeMissingCharacteristics = async (req, res) => {
  const sql = `
    SELECT
      '<a target="_blank" href="https://plants.sc.egov.usda.gov/home/plantProfile?symbol=' || a.plant_symbol || '">' || a.plant_symbol || '</a>'
        AS plant_symbol,
      a.cultivar,
      scientific,
      vernacular,
      ARRAY_AGG(DISTINCT state ORDER BY state) as states
    FROM (
      SELECT DISTINCT 
        plant_symbol,
        cultivar,
        full_scientific_name_without_author as scientific,
        primary_vernacular as vernacular
      FROM plants3.characteristics
      WHERE
        CASE WHEN full_scientific_name_without_author IS NULL THEN 0 ELSE 1 END +
        CASE WHEN primary_vernacular IS NULL THEN 0 ELSE 1 END +
        CASE WHEN plant_duration_name IS NULL THEN 0 ELSE 1 END +
        CASE WHEN plant_nativity_type IS NULL THEN 0 ELSE 1 END +
        CASE WHEN plant_nativity_region_name IS NULL THEN 0 ELSE 1 END +
        CASE WHEN plant_growth_habit_name IS NULL THEN 0 ELSE 1 END +
        CASE WHEN active_growth_period IS NULL THEN 0 ELSE 1 END +
        CASE WHEN after_harvest_regrowth_rate IS NULL THEN 0 ELSE 1 END +
        CASE WHEN bloat_potential IS NULL THEN 0 ELSE 1 END +
        CASE WHEN c_n_ratio IS NULL THEN 0 ELSE 1 END +
        CASE WHEN color_name IS NULL THEN 0 ELSE 1 END +
        CASE WHEN summer IS NULL THEN 0 ELSE 1 END +
        CASE WHEN winter IS NULL THEN 0 ELSE 1 END +
        CASE WHEN foliage_texture_name IS NULL THEN 0 ELSE 1 END +
        CASE WHEN growth_form_name IS NULL THEN 0 ELSE 1 END +
        CASE WHEN growth_rate IS NULL THEN 0 ELSE 1 END +
        CASE WHEN height_max_at_base_age IS NULL THEN 0 ELSE 1 END +
        CASE WHEN height_at_maturity IS NULL THEN 0 ELSE 1 END +
        CASE WHEN lifespan_name IS NULL THEN 0 ELSE 1 END +
        CASE WHEN toxicity_name IS NULL THEN 0 ELSE 1 END +
        CASE WHEN pd_max_range IS NULL THEN 0 ELSE 1 END +
        CASE WHEN ff_min_range IS NULL THEN 0 ELSE 1 END +
        CASE WHEN ph_min_range IS NULL THEN 0 ELSE 1 END +
        CASE WHEN ph_max_range IS NULL THEN 0 ELSE 1 END +
        CASE WHEN density_min_range IS NULL THEN 0 ELSE 1 END +
        CASE WHEN precip_min_range IS NULL THEN 0 ELSE 1 END +
        CASE WHEN precip_max_range IS NULL THEN 0 ELSE 1 END +
        CASE WHEN root_min_range IS NULL THEN 0 ELSE 1 END +
        CASE WHEN temp_min_range IS NULL THEN 0 ELSE 1 END +
        CASE WHEN anaerobic_tolerance IS NULL THEN 0 ELSE 1 END +
        CASE WHEN caco3_tolerance IS NULL THEN 0 ELSE 1 END +
        CASE WHEN drought_tolerance IS NULL THEN 0 ELSE 1 END +
        CASE WHEN fire_tolerance IS NULL THEN 0 ELSE 1 END +
        CASE WHEN hedge_tolerance IS NULL THEN 0 ELSE 1 END +
        CASE WHEN moisture_usage IS NULL THEN 0 ELSE 1 END +
        CASE WHEN soil_ph_tolerance_min IS NULL THEN 0 ELSE 1 END +
        CASE WHEN soil_ph_tolerance_max IS NULL THEN 0 ELSE 1 END +
        CASE WHEN precipitation_tolerance_min IS NULL THEN 0 ELSE 1 END +
        CASE WHEN precipitation_tolerance_max IS NULL THEN 0 ELSE 1 END +
        CASE WHEN salinity_tolerance IS NULL THEN 0 ELSE 1 END +
        CASE WHEN shade_tolerance_name IS NULL THEN 0 ELSE 1 END +
        CASE WHEN temperature_tolerance_min IS NULL THEN 0 ELSE 1 END +
        CASE WHEN bloom_period IS NULL THEN 0 ELSE 1 END +
        CASE WHEN fruit_seed_period_start IS NULL THEN 0 ELSE 1 END +
        CASE WHEN fruit_seed_period_end IS NULL THEN 0 ELSE 1 END +
        CASE WHEN seed_spread_rate IS NULL THEN 0 ELSE 1 END +
        CASE WHEN seedling_vigor IS NULL THEN 0 ELSE 1 END +
        CASE WHEN vegetative_spread_rate IS NULL THEN 0 ELSE 1 END +
        CASE WHEN palatability_browse IS NULL THEN 0 ELSE 1 END +
        CASE WHEN palatability_graze IS NULL THEN 0 ELSE 1 END +
        CASE WHEN protein_potential IS NULL THEN 0 ELSE 1 END +
        CASE WHEN frost_free_days_min IS NULL THEN 0 ELSE 1 END +
        CASE WHEN planting_density_min IS NULL THEN 0 ELSE 1 END +
        CASE WHEN root_depth_min IS NULL THEN 0 ELSE 1 END
        < 20
    ) a
    INNER JOIN (
      SELECT state, plant_symbol, cultivar_name
      FROM plants3.states
      WHERE
        plant_symbol || COALESCE(cultivar_name, '') NOT IN (
          SELECT plant_symbol || COALESCE(cultivar_name, '')
          FROM plants3.states
          WHERE state = 'all'
          GROUP BY plant_symbol || COALESCE(cultivar_name, '')
          HAVING COUNT(*) >= 20
        )
      GROUP BY state, plant_symbol, cultivar_name
      HAVING COUNT(*) < 20
    ) b
    ON a.plant_symbol = b.plant_symbol AND COALESCE(a.cultivar, '') = COALESCE(b.cultivar_name, '')
    GROUP BY a.plant_symbol, a.cultivar, scientific, vernacular
    ORDER BY 1, 2 NULLS FIRST;
  `;

  simpleQuery(sql, null, req, res);
};

const routeDataErrors = async (req, res) => {
  let s = `
    <style>
      body, table {
        font: 13px arial;
      }
    
      table {
        border: 1px solid black;
        border-spacing: 0; 
        empty-cells: show;
      }
      
      td, th {
        padding: 0.2em 0.5em;
        border-right: 1px solid #ddd;
        border-bottom: 1px solid #bbb;
      }

      th {
        background: #eee;
        position: sticky;
        top: 0;
      }

      h1 {
        font-size: 110%;
      }
    </style>
  `;

  const unknownMLRA = async () => {
    const validMlra = await (await fetch('https://polygons.vegspec.org/validmlra')).json();
    const stateData = await pool.query(`SELECT * FROM plants3.states WHERE parameter = 'mlra'`);
    const rows = [];
    const unknown = new Set();
    stateData.rows.forEach((row) => {
      const mlras = row.value.split(',');
      const u = [];
      mlras.forEach((mlra) => {
        if (!validMlra.includes(mlra)) {
          unknown.add(mlra);
          u.push(mlra);
        }
      });
      if (u.length) {
        row.mlra = u;
        rows.push(row);
      }
    });

    rows.sort((a, b) => (
      a.mlra.toString().localeCompare(b.mlra.toString())
      || a.value.localeCompare(b.value)
      || a.plant_symbol.localeCompare(b.plant_symbol)
      || a.state.localeCompare(b.state)
    ));

    const unknownSorted = [...unknown].sort((a, b) => (parseInt(a, 10) - parseInt(b, 10)) || a.localeCompare(b));

    s += `
      <h1>Unknown MLRAs:</h1>
      <ul>
        ${[...unknownSorted].map((u) => `<li>${u}</li>`).join('')}
      </ul>
      <h1>Complete listing:</h1>
      <table>
        <thead>
          <tr><th>#<th>state<th>plant_symbol<th>cultivar_name<th>MLRAs<th>Unknown</tr>
        </thead>
        <tbody>
          ${rows.map((row, i) => (`
            <tr>
              <td>${i + 1}</td>
              <td>${row.state}</td>
              <td>${row.plant_symbol}</td>
              <td>${row.cultivar_name || ''}</td>
              <td>${row.value}</td>
              <td>${row.mlra}</td>
            </tr>
          `)).join('')}
        </tbody>
      </table>
    `;
  }; // unknownMLRA
  await unknownMLRA();
  res.send(s);
};

const routeImageCredits = async (req, res) => {
  const { show, symbol } = req.query;

  const query = symbol
    ? `
      SELECT DISTINCT b.*, plant_image_location FROM (
        SELECT
          plant_symbol || '.' || imageid as plant_symbol,
          imagesizepath1, imagesizepath2, imagesizepath3,
          hascopyright,
          imagecreationdate, prefixname, datasourcetype,
          commonname, institutionname, credittype,
          emailaddress, literaturetitle, literatureyear, literatureplace, imageid,
          width, height
        FROM plants3.imagedata
        WHERE plant_symbol='${symbol}'
      ) b
      LEFT JOIN plants3.plant_image i
      ON b.imageid = i.plant_image_id
      ORDER BY imageid;
    `
    : `
      SELECT DISTINCT b.*, plant_image_location FROM (
        WITH first_images AS (
          SELECT DISTINCT ON (plant_symbol)
            plant_symbol, imagesizepath3
          FROM plants3.imagedata
          WHERE
            plant_symbol IN (
              SELECT DISTINCT psymbol from plants3.states a
              JOIN plants3.synonyms b
              ON plant_symbol = psymbol OR plant_symbol = ssymbol        
            )
            OR plant_symbol IN (
              SELECT DISTINCT ssymbol from plants3.states a
              JOIN plants3.synonyms b
              ON plant_symbol = psymbol OR plant_symbol = ssymbol        
            )
            OR plant_symbol IN (
              SELECT DISTINCT plant_symbol from plants3.states
            )
          ORDER BY plant_symbol, height::float / NULLIF(width, 0) NULLS LAST, imageid
        )
        SELECT
          i.plant_symbol, 
          i.imagesizepath1, i.imagesizepath2, i.imagesizepath3,
          i.hascopyright,
          i.imagecreationdate, i.prefixname, i.datasourcetype,
          i.commonname, i.institutionname, i.credittype,
          i.emailaddress, i.literaturetitle, i.literatureyear, i.literatureplace, i.imageid,
          i.plant_image_id,
          i.width, i.height
        FROM plants3.imagedata i
        JOIN first_images f
        ON i.plant_symbol = f.plant_symbol
          AND i.imagesizepath3 = f.imagesizepath3
          AND i.prefixname IS DISTINCT FROM 'Scanned by'
        ORDER BY i.plant_symbol
      ) b
      LEFT JOIN plants3.plant_image i
      ON b.imageid = i.plant_image_id
    `;

  const results = await pool.query(query);

  const data = {};
  results.rows.forEach((row) => {
    data[row.plant_symbol] = data[row.plant_symbol] || {
      thumbnail: row.imagesizepath3?.split('\\').slice(-1)[0],
      standard: row.imagesizepath1?.split('\\').slice(-1)[0],
      large: row.imagesizepath2?.split('\\').slice(-1)[0],
      copyright: row.hascopyright === 'HasCopyright',
      holder: '',
      artist: '',
      author: '',
      prefix: '',
      other: '',
      provider: '',
    };

    const obj = data[row.plant_symbol];

    obj[row.credittype.toLowerCase()] = row.commonname;
    obj[row.datasourcetype.toLowerCase()] = row.commonname;
    obj.prefix = data[row.plant_symbol].prefix || row.prefixname;
    obj.width = row.width;
    obj.height = row.height;
    obj.location = row.plant_image_location;
    if (row.artist) obj.artist = row.artist;
    if (row.author) obj.artist = row.author;
    if (row.prefixname === 'Provided by') obj.provider = `Provided by ${row.commonname}`;
    if (row.literaturetitle) obj.title = row.literaturetitle;
    if (+row.literatureyear) obj.year = row.literatureyear;
    if (row['copyright holder']) obj.holder = row['copyright holder'];
  });

  const output = symbol ? [] : {};
  Object.keys(data).forEach((key) => {
    const d = data[key];
    output[key] = {
      thumbnail: d.thumbnail,
      standard: d.standard,
      large: d.large,
      width: d.width,
      height: d.height,
      copyright: d.copyright,
      author: d.artist || d.holder || d.author,
    };

    output[key].description = `
      ${[...new Set([d.artist, d.holder, d.author])].filter((s) => s.trim()).join('. ')}.
      ${[d.year, d.title, d.provider].filter((s) => s?.toString() && ![d.artist, d.holder, d.author].includes(s)).join(', ')}
      ${d.location ? `. ${d.location}` : ''}
    `.replace(/[\n\r]+\s+/g, ' ')
      .trim()
      .replace(/^\.\s*/, '')
      .replace(/\s*\./g, '.')
      .replace(/\.\s*\./g, '')
      .trim();
  });

  const synonyms = await pool.query('SELECT DISTINCT psymbol, ssymbol from plants3.synonyms');

  // res.send(synonyms.rows); return;
  synonyms.rows.forEach((row) => {
    // Prioritize on landscape, even if it means changing to a synonym's image. But avoid black and white (svd)
    if (output[row.psymbol] && output[row.ssymbol]) {
      if (
        (output[row.psymbol].width / output[row.psymbol].height < output[row.ssymbol].width / output[row.ssymbol].height)
        || output[row.psymbol].standard.includes('svd.')
      ) {
        output[row.psymbol] = output[row.ssymbol];
      } else if (output[row.ssymbol].standard.includes('svd.')) {
        output[row.ssymbol] = output[row.psymbol];
      }
    }

    if (!output[row.psymbol]) {
      output[row.psymbol] = output[row.ssymbol];
    } else if (!output[row.ssymbol]) {
      output[row.ssymbol] = output[row.psymbol];
    }
  });

  const sorted = Object.keys(output).sort().filter((key) => output[key]).reduce((obj, key) => {
    obj[key] = output[key];
    return obj;
  }, {});

  if (show) {
    if (show === 'portrait') {
      const done = {};
      Object.entries(sorted).forEach(([key, value]) => {
        if (value.width > value.height || done[value.standard]) {
          delete sorted[key];
        }
        done[value.standard] = true;
      });

      let html = `
        <html>
          <style>
            body {
              background: #ddd;
            }

            img {
              height: 100px;
            }

            table {
              border: 1px solid black;
              border-spacing: 0; 
              empty-cells: show;
              font-family: verdana;
              font-size: 13px;
              margin: auto;
            }
            
            tr:nth-child(1) th {
              position: sticky;
              top: 0;
              z-index: 1;
            }

            td, th {
              border-right: 1px solid #ddd;
              border-bottom: 1px solid #bbb;
            }
            
            th {
              background: #333;
              color: #eee;
              padding: 0.3rem;
            }

            td:nth-child(1) {
              border-right: 3px ridge;
              width: 235px;
              text-align: center;
            }

            td div {
              position: relative;
              overflow: hidden;
              height: 100px;
              width: 235px;
            }
            
            td:nth-child(2) img {
              position: absolute;
              top: 50%;
              left: 50%;
              transform: translate(-50%, -50%);
              min-width: 100%;
              min-height: 100%;
              object-fit: cover;
            }

            td:nth-child(3) img {
              position: absolute;
              top: 0;
              left: 50%;
              transform: translateX(-50%);
              min-width: 100%;
              min-height: 100%;
              object-fit: cover;
              object-position: top;
            }

            td:nth-child(3) img {
              position: absolute;
              top: 0;
              left: 50%;
              transform: translateX(-50%);
              min-width: 100%;
              min-height: 100%;
              object-fit: cover;
              object-position: top;
            }

            td:nth-child(4) img {
              position: absolute;
              top: 0;
              left: 50%;
              transform: translateX(-50%);
              min-width: 100%;
              min-height: 100%;
              object-fit: cover;
              object-position: center 25%;
            }

            table a {
              color: white;
            }
          </style>

          <body>
            <table>
              <tr>
                <th>Original</th>
                <th>Landscape<br>Centered</th>
                <th>Landscape<br>Top</th>
                <th>Landscape<br>Down 25%</th>
              </tr>
      `;

      html += Object.entries(sorted).map(([key, value]) => (`
        <tr>
          <th colspan="4">
            <a target="_blank" href="https://plants.sc.egov.usda.gov/plant-profile/${key}/images">
              ${key}
            </a>
          </th>
        </tr>
        <tr>
          <td><img src="https://plants.sc.egov.usda.gov/ImageLibrary/standard/${value.standard}"></td>
          <td><div><img src="https://plants.sc.egov.usda.gov/ImageLibrary/standard/${value.standard}"></div></td>
          <td><div><img src="https://plants.sc.egov.usda.gov/ImageLibrary/standard/${value.standard}"></div></td>
          <td><div><img src="https://plants.sc.egov.usda.gov/ImageLibrary/standard/${value.standard}"></div></td>
        </tr>
      `)).join('');

      res.send(html);
    }
  } else {
    res.send(sorted);
  }

  // res.send(data);
  // res.send(results.rows);
};

const routeImageSizes = async (req, res) => {
  const results = await pool.query(`
    SELECT DISTINCT imagesizepath3
    FROM plants3.imagedata
    WHERE width IS NULL
    ORDER BY 1
  `);

  res.send(`Determining image dimensions for ${results.rows.length} images`);

  let url;
  for (const row of results.rows) {
    try {
      url = `https://plants.sc.egov.usda.gov${row.imagesizepath3}`;
      const response = await fetch(url);
      const arrayBuffer = await response.arrayBuffer();
      const buffer = Buffer.from(arrayBuffer); // Convert to Node.js Buffer
      const { width, height } = await sharp(buffer).metadata();

      await pool.query(
        `UPDATE plants3.imagedata
         SET width = $1, height = $2
         WHERE imagesizepath3 = $3`,
        [width, height, row.imagesizepath3],
      );
      await pool.query('SELECT pg_sleep(0.5);');
    } catch (err) {
      console.error(`Error with image ${url}:`, err.message);
    }
  }
}; // routeImageSizes

const routeInvalidMLRA = async (req, res) => {
  const query = `
    SELECT DISTINCT state, s.plant_symbol as symbol, s.parameter, s.value, bad.mlra AS invalid_mlra
    FROM plants3.states s
    JOIN LATERAL regexp_split_to_table(s.value, ',') AS bad(mlra) ON TRUE
    LEFT JOIN mlra.mlra valid ON bad.mlra = valid.mlrarsym
    WHERE
      (s.parameter = 'mlra' AND valid.mlrarsym IS NULL)
      AND bad.mlra != '95'
    ORDER BY 5, 1;
  `;

  const results = await pool.query(query);
  sendResults(req, res, results.rows);
}; // routeInvalidMLRA

const routeInvalidCPS = async (req, res) => {
  const query = `
    WITH valid(cps) AS (
      VALUES
        ('311'), ('327'), ('332'), ('340'), ('342'), ('379'),
        ('380'), ('381'), ('386'), ('390'), ('391'), ('393'),
        ('395'), ('412'), ('420'), ('422'), ('512'), ('550'),
        ('580'), ('601'), ('603'), ('612'), ('635'), ('643'),
        ('644'), ('645'), ('647'), ('657'), ('658'), ('659'),
        ('810')
    )
    SELECT DISTINCT
      state,
      plant_symbol as symbol,
      s.parameter,
      s.value,
      bad.cps AS invalid_cps
    FROM plants3.states s
    JOIN LATERAL regexp_split_to_table(s.value, ',') AS bad(cps) ON TRUE
    LEFT JOIN valid ON bad.cps = valid.cps
    WHERE
      s.parameter = 'cps'
      AND valid.cps IS NULL
    ORDER BY 5, 1;
  `;

  const results = await pool.query(query);
  sendResults(req, res, results.rows);
}; // routeInvalidCPS

const routeInvalidSeedPerPound = async (req, res) => {
  const query = `
    SELECT DISTINCT
      plant_symbol AS symbol,
      cultivar,
      c.seed_per_pound AS correct_seed_per_pound,
      r.seed_per_pound as plants_seed_per_pound
    FROM plants3.characteristics c
    INNER JOIN plants3.plant_reproduction r
    ON
      c.plant_master_id = r.plant_master_id
      AND COALESCE(c.cultivar, '') = COALESCE(r.cultivar_name, '')
    WHERE c.seed_per_pound <> r.seed_per_pound
    ORDER BY 1, 2;
  `;

  const results = await pool.query(query);
  sendResults(req, res, results.rows);
}; // routeInvalidSeedPerPound

const routeMixes = async (req, res) => {
  const query = 'SELECT * FROM vegspec.mixes;';
  const results = await pool.query(query);
  sendResults(req, res, results.rows);
}; // routeMixes

module.exports = {
  routeCharacteristics,
  routeProps,
  routeSymbols,
  routeNewSpecies,
  routeRenameCultivar,
  routeRecords,
  routeStructure,
  routeSaveState,
  routeDeleteState,
  routeState,
  routeEditState,
  routePlantsEmptyColumns,
  routePlantsTable,
  routeMissingCultivars,
  routeMoveCultivar,
  routeDatabaseChanges,
  routeRetention,
  routeMissingCharacteristics,
  routeValidStates,
  routeDataErrors,
  routeImageCredits,
  routeImageSizes,
  routeInvalidMLRA,
  routeInvalidCPS,
  routeInvalidSeedPerPound,
  routeMixes,
};
