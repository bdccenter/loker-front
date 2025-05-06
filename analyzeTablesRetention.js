// analyzeTablesRetention.js
// Script para analizar las estructuras de las tablas de retención en BigQuery
import { BigQuery } from '@google-cloud/bigquery';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Obtener la ruta del archivo actual
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Ruta a las credenciales de BigQuery
const CREDENTIALS_PATH = path.join(__dirname, '../../credentials/bigquery-key.json');

// Configuración de tablas a analizar por agencia
const agencyTables = {
  'Gran Auto': {
    projectId: 'base-maestra-gn',
    datasetName: 'Posventa',
    tableName: 'tab_bafac_ur'
  },
  'Del Bravo': {
    projectId: 'base-maestra-delbravo',
    datasetName: 'Posventa',
    tableName: 'tab_bafac_ur'
  },
  'Sierra': {
    projectId: 'base-maestra-sierra',
    datasetName: 'Posventa',
    tableName: 'tab_bafac_ur'
  },
  'Huerpel': {
    projectId: 'base-maestra-huerpel',
    datasetName: 'Posventas', // Con 's' al final
    tableName: 'tab_bafac_ur'
  },
  'Gasme': {
    projectId: 'gasme-bi',
    datasetName: 'Posventa',
    tableName: 'tab_bafac_ur'
  }
};

// Campos comunes e importantes que queremos identificar en cada tabla
const keyFields = [
  // Identificación
  'Vin', 'SERIE', 'Branch', 'VINAno_', 'orden', 'ORDEN',
  
  // Cliente
  'NombreCliente', 'NOMBRE_CLI', 'NOMBRE_CLIENTE', 'NOMBRE_FAC', 'CLIENTE',
  
  // Vehículo
  'Modelo_', 'MODELO', 'AnioVeh_', 'AÑO_MODELO', 'ANIO_VIN',
  
  // Fecha y tiempo
  'Date', 'FECHA_FAC', 'ULT_VISITA', 'DIAS_SIN_VENIR',
  
  // Contacto
  'Email', 'CORREO', 'Telefono', 'TELEFONO', 'CELULAR',
  
  // Asesor
  'NombreAsesor', 'NOMBRE_ASESOR', 'ASESOR',
  
  // Financieros
  'CostoTotal12M_', 'VentaTotal12M_', 'TOTAL', 'CostoMO12M_', 'CostoPartesOrig12M_'
];

// Palabras clave para campos de fecha
const dateKeywords = ['DATE', 'FECHA', 'VISITA', 'ULT'];

/**
 * Analiza la estructura de una tabla específica en BigQuery
 */
async function analyzeTable(agencyName) {
  const config = agencyTables[agencyName];
  if (!config) {
    console.error(`No hay configuración para la agencia: ${agencyName}`);
    return null;
  }
  
  console.log(`\n\n========= ANÁLISIS DE TABLA: ${agencyName} =========`);
  console.log(`Proyecto: ${config.projectId}`);
  console.log(`Dataset: ${config.datasetName}`);
  console.log(`Tabla: ${config.tableName}`);
  
  try {
    // Verificar que existe el archivo de credenciales
    if (!fs.existsSync(CREDENTIALS_PATH)) {
      throw new Error(`No se encontró el archivo de credenciales en: ${CREDENTIALS_PATH}`);
    }
    
    // Crear cliente BigQuery
    const bigquery = new BigQuery({
      projectId: config.projectId,
      keyFilename: CREDENTIALS_PATH
    });
    
    // 1. Obtener la estructura de la tabla (metadatos)
    console.log(`\nObteniendo estructura de la tabla...`);
    
    try {
      const [metadata] = await bigquery.dataset(config.datasetName).table(config.tableName).getMetadata();
      
      console.log(`\n✅ Tabla encontrada con ${metadata.schema.fields.length} columnas`);
      
      // Mostrar todas las columnas
      console.log(`\nLista completa de columnas:`);
      metadata.schema.fields.forEach(field => {
        console.log(`  - ${field.name} (${field.type})`);
      });
      
      // Identificar campos clave importantes para la aplicación
      const foundKeyFields = metadata.schema.fields
        .filter(field => keyFields.some(keyField => 
          field.name.toUpperCase().includes(keyField.toUpperCase())
        ))
        .map(field => ({ name: field.name, type: field.type }));
      
      console.log(`\nCampos clave encontrados (${foundKeyFields.length}):`);
      foundKeyFields.forEach(field => {
        console.log(`  - ${field.name} (${field.type})`);
      });
      
      // Identificar campo de fecha para calcular días sin venir
      const dateFields = metadata.schema.fields
        .filter(field => 
          dateKeywords.some(keyword => field.name.toUpperCase().includes(keyword)) &&
          (field.type === 'DATE' || field.type === 'TIMESTAMP' || field.type === 'DATETIME')
        )
        .map(field => ({ name: field.name, type: field.type }));
      
      console.log(`\nPosibles campos de fecha para calcular días sin venir (${dateFields.length}):`);
      dateFields.forEach(field => {
        console.log(`  - ${field.name} (${field.type})`);
      });
      
      // 2. Obtener ejemplos de datos
      console.log(`\nObteniendo ejemplos de datos...`);
      
      const query = `
        SELECT *
        FROM \`${config.projectId}.${config.datasetName}.${config.tableName}\`
        LIMIT 5
      `;
      
      const [rows] = await bigquery.query({ query });
      
      if (rows && rows.length > 0) {
        console.log(`\n✅ Datos obtenidos: ${rows.length} filas de ejemplo`);
        
        // Mostrar los nombres de las columnas que contienen datos
        const columnsWithData = Object.keys(rows[0]);
        console.log(`\nColumnas con datos (${columnsWithData.length}):`);
        columnsWithData.forEach(col => {
          const examples = rows.map(row => row[col]).filter(Boolean);
          const example = examples.length > 0 ? examples[0] : 'NULL';
          console.log(`  - ${col}: ${typeof example === 'object' ? JSON.stringify(example) : example}`);
        });
        
        // 3. Generar ejemplo de consulta SQL para esta tabla
        const bestDateField = dateFields.length > 0 ? dateFields[0].name : null;
        
        console.log(`\n📝 CONSULTA SQL RECOMENDADA:`);
        
        let sqlQuery = `
SELECT
  ${columnsWithData.join(',\n  ')}`;
        
        if (bestDateField) {
          sqlQuery += `,
  FORMAT_DATE('%d %b %Y', ${bestDateField}) as ULT_VISITA,
  CASE
    WHEN ${bestDateField} IS NULL THEN NULL
    ELSE DATE_DIFF(CURRENT_DATE(), CAST(${bestDateField} AS DATE), DAY)
  END as DIAS_SIN_VENIR`;
        }
        
        sqlQuery += `
FROM
  \`${config.projectId}.${config.datasetName}.${config.tableName}\`
ORDER BY
  ${bestDateField || columnsWithData[0]} DESC
LIMIT 1500`;
        
        console.log(sqlQuery);
        
        return {
          fields: metadata.schema.fields.map(f => ({ name: f.name, type: f.type })),
          keyFields: foundKeyFields,
          dateFields,
          bestDateField,
          sampleColumns: columnsWithData,
          recommendedQuery: sqlQuery
        };
      } else {
        console.log(`No se encontraron datos en la tabla.`);
        return {
          fields: metadata.schema.fields.map(f => ({ name: f.name, type: f.type })),
          keyFields: foundKeyFields,
          dateFields,
          bestDateField: dateFields.length > 0 ? dateFields[0].name : null,
          sampleColumns: [],
          recommendedQuery: null
        };
      }
      
    } catch (error) {
      console.error(`Error al obtener metadata: ${error.message}`);
      return null;
    }
    
  } catch (error) {
    console.error(`Error al analizar la tabla: ${error.message}`);
    return null;
  }
}

/**
 * Función principal para analizar todas las tablas
 */
async function analyzeAllTables() {
  console.log(`🔍 INICIANDO ANÁLISIS DE TABLAS DE RETENCIÓN`);
  console.log(`==============================================`);
  
  const results = {};
  
  for (const agency of Object.keys(agencyTables)) {
    try {
      const result = await analyzeTable(agency);
      results[agency] = result;
    } catch (error) {
      console.error(`Error analizando ${agency}: ${error.message}`);
    }
  }
  
  // Guardar resultados del análisis en un archivo JSON
  const resultsPath = path.join(__dirname, 'table_analysis_results.json');
  fs.writeFileSync(resultsPath, JSON.stringify(results, null, 2));
  console.log(`\n✅ Resultados del análisis guardados en: ${resultsPath}`);
  
  // Generar configuración recomendada para updateCSV_retencion.js
  const configPath = path.join(__dirname, 'recommended_retention_config.js');
  
  let configContent = `// Configuración recomendada para las tablas de retención
const agencyConfig = {
`;
  
  for (const [agency, result] of Object.entries(results)) {
    if (!result) continue;
    
    const dateField = result.bestDateField || 'Date';
    
    configContent += `  '${agency}': {
    fileName: '${agency.toLowerCase().replace(/\s+/g, '')}_ret.csv',
    projectId: '${agencyTables[agency].projectId}',
    datasetName: '${agencyTables[agency].datasetName}',
    tableName: '${agencyTables[agency].tableName}',
    encoding: 'utf8',
    dateField: '${dateField}',
    dateFormat: '%d %b %Y'
  },
`;
  }
  
  configContent += `};

export default agencyConfig;`;
  
  fs.writeFileSync(configPath, configContent);
  console.log(`\n✅ Configuración recomendada guardada en: ${configPath}`);
}

// Ejecutar el análisis si se llama directamente
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  analyzeAllTables()
    .then(() => {
      console.log("\n🎉 Análisis completado");
      process.exit(0);
    })
    .catch(error => {
      console.error("Error en el análisis:", error);
      process.exit(1);
    });
}

export { analyzeTable, analyzeAllTables };