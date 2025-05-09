// src/config/agencyConfig.js

/**
 * Configuración para cada agencia
 * Incluye: 
 * - fileName: nombre del archivo CSV a generar
 * - projectId: proyecto en BigQuery
 * - datasetName: dataset específico de la agencia
 * - tableName: tabla que contiene los datos en BigQuery
 * - encoding: codificación específica para el CSV
 */
const agencyConfig = {
  // Configuración para cada agencia
  // Incluye el nombre del archivo CSV, el proyecto de BigQuery, el dataset y la tabla
  // Además, se especifica la codificación para cada agencia
    'Gran Auto': {
      fileName: 'granauto.csv',
      projectId: 'base-maestra-gn',
      datasetName: 'BASE_MAESTRA',
      tableName: 'tab_bafac_ur',
      encoding: 'cp1252' // Codificación específica para Gran Auto
    },
    'Del Bravo': {
      fileName: 'delbravo.csv',
      projectId: 'base-maestra-delbravo',
      datasetName: 'Posventa',
      tableName: 'tab_bafac_ur',
      encoding: 'utf-8'
    },
    'Sierra': {
      fileName: 'sierra.csv',
      projectId: 'base-maestra-sierra',
      datasetName: 'Posventa',
      tableName: 'tab_bafac_ur',
      encoding: 'utf-8'
    },
    'Huerpel': {
      fileName: 'huerpel.csv',
      projectId: 'base-maestra-huerpel',
      datasetName: 'Posventas',
      tableName: 'tab_bafac_ur',
      encoding: 'utf-8'
    }
  };
  
  /**
   * Genera la consulta SQL para una agencia específica
   * @param {string} agency - Nombre de la agencia
   * @returns {string} - Consulta SQL generada
   */
  // Esta función genera la consulta SQL para una agencia específica
  function generateQuery(agency) {
    const config = agencyConfig[agency];
    if (!config) throw new Error(`Configuración no encontrada para la agencia: ${agency}`);
    // Se construye el nombre completo de la tabla en BigQuery
    const fullTableName = `\`${config.projectId}.${config.datasetName}.${config.tableName}\``;
    
    // Consulta base para todas las agencias
    // Ajusta los campos según los datos reales de tu tabla
    return `
      SELECT
        ORDEN,
        SERIE,
        MODELO,
        ANIO_VIN,
        NOMBRE_FAC,
        CONTACTO,
        AGENCIA,
        CELULAR,
        TELEFONO,
        OFICINA,
        PAQUETE,
        TOTAL,
        NOMBRE_ASESOR,
        ULT_VISITA,
        DIAS_NOSHOW
      FROM
        ${fullTableName}
      ORDER BY
        ULT_VISITA DESC
    `;
  }
  
  export { agencyConfig, generateQuery };