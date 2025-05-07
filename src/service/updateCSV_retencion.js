// src/service/updateCSV_retencion.js
import { executeQuery, saveToCSV, saveToCSVWithRetry } from './bigQueryService.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { splitCSVFile } from '../utils/splitCSV.js';
import { compressCSV } from '../utils/compressCSV.js';

// Obtener la ruta del archivo actual
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);


// Ruta para logs
const LOGS_DIR = path.join(__dirname, '../../logs');
if (!fs.existsSync(LOGS_DIR)) {
  fs.mkdirSync(LOGS_DIR, { recursive: true });
}

// Configuración actualizada de agencias para tablas de retención
// Basado en los errores observados y la estructura real de las tablas
const agencyConfig = {
  'Gran Auto': {
    fileName: 'granauto.csv', // Modificado según requisito (sin _ret)
    projectId: 'base-maestra-gn',
    datasetName: 'Posventa',
    tableName: 'tab_bafac_ur',
    encoding: 'utf8',
    dateField: 'FECHA_FAC', // Nombre correcto del campo de fecha según los errores
    dateFormat: '%d/%m/%Y' // Cambiado al formato DD/MM/YYYY
  },
  'Del Bravo': {
    fileName: 'delbravo.csv',
    // projectId: 'base-maestra-gn', 
    projectId: 'base-maestra-delbravo',
    datasetName: 'Posventa',
    tableName: 'tab_bafac_ur',
    encoding: 'utf8',
    dateField: 'FECHA_FAC',
    dateFormat: '%d/%m/%Y',
    // El filtro está bien definido, se aplicará a la columna AGENCI o AGENCIA de la tabla correcta
    agencyFilter: ['Acuña', 'Piedras Negras', 'Sabinas']
  },
  'Sierra': {
    fileName: 'sierra.csv', // Modificado según requisito
    projectId: 'base-maestra-sierra',
    datasetName: 'Posventa',
    tableName: 'tab_bafac_ur',
    encoding: 'utf8',
    dateField: 'FECHA_FAC', // Campo confirmado que existe
    dateFormat: '%d/%m/%Y' // Cambiado al formato DD/MM/YYYY
  },
  'Huerpel': {
    fileName: 'huerpel.csv',
    projectId: 'base-maestra-huerpel',
    datasetName: 'Posventas',
    tableName: 'tab_bafac_ur',
    encoding: 'utf8',
    dateField: 'FECHA_FACT', // Corregido según los campos que mostraste
    dateFormat: '%d/%m/%Y'
  },
  'Gasme': {
    fileName: 'gasme.csv', // Modificado según requisito
    projectId: 'base-maestra-gn', // Cambiado para usar el proyecto con acceso
    datasetName: 'Posventa',
    tableName: 'tab_bafac_ur',
    encoding: 'utf8',
    dateField: 'FECHA_FAC',
    dateFormat: '%d/%m/%Y' // Cambiado al formato DD/MM/YYYY
  }
};

/**
 * Genera una consulta SQL para extraer datos de la tabla de retención
 * @param {string} agency - Nombre de la agencia
 * @returns {string} - Consulta SQL generada
 */
function generateQuery(agencyName) {
  const config = agencyConfig[agencyName];
  if (!config) throw new Error(`Configuración no encontrada para la agencia: ${agencyName}`);

  const dateField = config.dateField || 'FECHA_FAC';
  const dateFormat = config.dateFormat || '%d/%m/%Y';

  // Construir la cláusula WHERE para filtrar agencias específicas si es necesario
  let whereClause = '';
  if (config.agencyFilter && config.agencyFilter.length > 0) {
    // Crear la condición IN para filtrar por agencias específicas
    const agenciesQuoted = config.agencyFilter.map(a => `'${a}'`).join(', ');

    // Usar AGENCIA para DelBravo, Huerpel y Sierra, AGENCI para el resto
    if (['base-maestra-delbravo', 'base-maestra-huerpel', 'base-maestra-sierra'].includes(config.projectId)) {
      whereClause = `WHERE AGENCIA IN (${agenciesQuoted})`;
    } else {
      whereClause = `WHERE AGENCI IN (${agenciesQuoted})`;
    }
  }

  // Consulta SQL con filtro condicional
  return `
  SELECT
    *,
    FORMAT_DATE('${dateFormat}', ${dateField}) as ULT_VISITA,
    CASE
      WHEN ${dateField} IS NULL THEN NULL
      ELSE DATE_DIFF(CURRENT_DATE(), CAST(${dateField} AS DATE), DAY)
    END as DIAS_SIN_VENIR
  FROM
    \`${config.projectId}.${config.datasetName}.${config.tableName}\`
  ${whereClause}
  ORDER BY
    ${dateField} DESC`;
}


/**
 * Limpia la carpeta Public eliminando todos los archivos existentes
 * @returns {Promise<void>}
 */
async function limpiarCarpetaPublic() {
  try {
    const publicPath = path.join(__dirname, '../../public');
    console.log(`Limpiando carpeta Public: ${publicPath}`);

    // Verificar que la carpeta existe
    if (!fs.existsSync(publicPath)) {
      console.log('La carpeta Public no existe, creándola...');
      fs.mkdirSync(publicPath, { recursive: true });
      return;
    }

    // Leer todos los archivos de la carpeta
    const archivos = fs.readdirSync(publicPath);
    console.log(`Se encontraron ${archivos.length} archivos en la carpeta Public`);

    // Eliminar cada archivo
    let eliminados = 0;
    let errores = 0;

    for (const archivo of archivos) {
      try {
        const rutaArchivo = path.join(publicPath, archivo);
        // Verificar si es un archivo (no una carpeta)
        if (fs.statSync(rutaArchivo).isFile()) {
          fs.unlinkSync(rutaArchivo);
          eliminados++;
        }
      } catch (error) {
        console.error(`Error al eliminar el archivo ${archivo}: ${error.message}`);
        errores++;
      }
    }

    console.log(`✅ Carpeta Public limpiada: ${eliminados} archivos eliminados, ${errores} errores`);
  } catch (error) {
    console.error('Error al limpiar la carpeta Public:', error);
  }
}

/**
 * Formatea una fecha al formato español (DD/MM/YYYY)
 * @param {Date|string|Object} dateValue - Valor de fecha a formatear
 * @returns {string} - Fecha formateada
 */
function formatDateToSpanish(dateValue) {
  try {
    let date;

    // Convertir el valor a objeto Date
    if (dateValue instanceof Date) {
      date = dateValue;
    } else if (typeof dateValue === 'string') {
      // Manejar diferentes formatos de fecha en string
      if (dateValue.includes('-')) {
        // Formato ISO: YYYY-MM-DD
        const parts = dateValue.split('-');
        if (parts.length === 3) {
          const year = parseInt(parts[0], 10);
          const month = parseInt(parts[1], 10) - 1; // Los meses en JS van de 0 a 11
          const day = parseInt(parts[2], 10);
          date = new Date(year, month, day);
        } else {
          date = new Date(dateValue);
        }
      } else if (dateValue.includes('/')) {
        // Formato: DD/MM/YYYY o MM/DD/YYYY
        const parts = dateValue.split('/');
        if (parts.length === 3) {
          // Asumimos formato DD/MM/YYYY por ser más común en español
          const day = parseInt(parts[0], 10);
          const month = parseInt(parts[1], 10) - 1;
          const year = parseInt(parts[2], 10);
          date = new Date(year, month, day);
        } else {
          date = new Date(dateValue);
        }
      } else {
        // Cualquier otro formato
        date = new Date(dateValue);
      }
    } else if (typeof dateValue === 'object' && dateValue !== null) {
      // Intentar extraer valor de fecha de un objeto
      if (dateValue.value) {
        return formatDateToSpanish(dateValue.value);
      }

      // Si es un objeto fecha de BigQuery (tiene getFullYear, getMonth, etc.)
      if (typeof dateValue.getFullYear === 'function') {
        date = new Date(
          dateValue.getFullYear(),
          dateValue.getMonth(),
          dateValue.getDate()
        );
      } else {
        // Intentar convertir el objeto a string y luego a fecha
        const dateStr = String(dateValue);
        date = new Date(dateStr);
      }
    } else {
      // Si no es ninguno de los tipos anteriores, devolver vacío
      return '';
    }

    // Comprobar si es una fecha válida
    if (isNaN(date.getTime())) {
      return '';
    }

    // Formatear según el patrón DD/MM/YYYY
    const day = date.getDate().toString().padStart(2, '0');
    const month = (date.getMonth() + 1).toString().padStart(2, '0'); // +1 porque los meses van de 0 a 11
    const year = date.getFullYear();

    return `${day}/${month}/${year}`;
  } catch (error) {
    console.error('Error al formatear fecha:', error);
    return '';
  }
}

/**
 * Intenta extraer y estandarizar columnas importantes
 * @param {Array} data - Datos a procesar
 * @param {string} agency - Nombre de la agencia
 * @returns {Array} - Datos procesados
 */
function standardizeColumns(data, agency) {
  if (!data || data.length === 0) return data;

  return data.map(row => {
    const standardRow = { ...row };

    // Estandarización de serie
    if (!standardRow.SERIE && standardRow.Vin) {
      standardRow.SERIE = standardRow.Vin;
    }

    // Estandarización de cliente
    if (!standardRow.NOMBRE_CLI && standardRow.NombreCliente) {
      standardRow.NOMBRE_CLI = standardRow.NombreCliente;
    }

    // Estandarización de modelo
    if (!standardRow.MODELO && standardRow.Modelo_) {
      standardRow.MODELO = standardRow.Modelo_;
    }

    // Estandarización de año del vehículo
    if (!standardRow.ANIO_VIN && standardRow.AnioVeh_) {
      standardRow.ANIO_VIN = standardRow.AnioVeh_;
    }

    // Estandarización de teléfono
    if (!standardRow.TELEFONO && standardRow.Telefono) {
      standardRow.TELEFONO = standardRow.Telefono;
    }

    return standardRow;
  });
}

/**
 * Actualiza el CSV para una agencia específica
 * @param {string} agencyName - Nombre de la agencia a actualizar
 * @returns {Promise<Object>} - Resultado de la operación
 */
/**
 * Actualiza el CSV para una agencia específica
 * @param {string} agencyName - Nombre de la agencia a actualizar
 * @returns {Promise<Object>} - Resultado de la operación
 */
async function updateAgencyCSV(agencyName) {
  try {
    console.log(`Iniciando actualización de datos de retención para ${agencyName}...`);

    if (!agencyConfig[agencyName]) {
      throw new Error(`Agencia no configurada: ${agencyName}`);
    }

    const config = agencyConfig[agencyName];

    // Generar la consulta SQL
    const query = generateQuery(agencyName);
    console.log(`\nConsulta generada para ${agencyName}:`);
    console.log(query);

    // Ejecutar la consulta en BigQuery
    let data;
    try {
      data = await executeQuery(config.projectId, query);
      console.log(`✅ Consulta exitosa para ${agencyName}: ${data.length} registros obtenidos`);

      // Mostrar los nombres de los campos obtenidos
      if (data.length > 0) {
        const headers = Object.keys(data[0]);
        console.log(`Encabezados detectados: ${headers.join(', ')}`);
      }
    } catch (queryError) {
      console.error(`Error en la consulta para ${agencyName}:`, queryError.message);

      // Si la consulta falla, intentar con una versión más simplificada
      const backupQuery = `
        SELECT *
        FROM \`${config.projectId}.${config.datasetName}.${config.tableName}\`
      `;

      console.log(`Reintentando con consulta más simple para ${agencyName}...`);
      console.log(backupQuery);

      try {
        data = await executeQuery(config.projectId, backupQuery);
        console.log(`✅ Consulta de respaldo exitosa: ${data.length} registros obtenidos`);

        // Calcular DIAS_SIN_VENIR manualmente después
        if (data.length > 0 && data[0][config.dateField]) {
          data = data.map(row => {
            if (row[config.dateField]) {
              try {
                const fechaUltVisita = new Date(row[config.dateField]);
                const fechaActual = new Date();
                const diffTime = Math.abs(fechaActual - fechaUltVisita);
                const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                row.DIAS_SIN_VENIR = diffDays;
                row.ULT_VISITA = formatDateToSpanish(fechaUltVisita);
              } catch (e) {
                console.warn(`No se pudo calcular DIAS_SIN_VENIR para un registro: ${e.message}`);
                row.DIAS_SIN_VENIR = null;
              }
            }
            return row;
          });
        }
      } catch (backupError) {
        throw new Error(`No se pudo ejecutar la consulta original ni la de respaldo: ${backupError.message}`);
      }
    }

    if (!data || data.length === 0) {
      console.warn(`No se encontraron datos para ${agencyName}`);
      return {
        agency: agencyName,
        success: false,
        error: 'No se encontraron datos',
        timestamp: new Date().toISOString()
      };
    }

    // Estandarizar nombres de columnas
    const standardizedData = standardizeColumns(data, agencyName);

    // Procesar los datos para asegurar formato correcto de fechas
    const processedData = standardizedData.map(row => {
      // Crear una copia del objeto para no modificar el original
      const processedRow = { ...row };

      // Procesar todas las propiedades que parecen fechas
      Object.keys(processedRow).forEach(key => {
        const value = processedRow[key];

        // Si parece una fecha (objeto o string con formato de fecha)
        if (
          (typeof value === 'object' && value !== null) ||
          (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value))
        ) {
          try {
            // Intentar formatear como fecha
            const formattedDate = formatDateToSpanish(value);
            if (formattedDate) {
              processedRow[key] = formattedDate;
            }
          } catch (e) {
            // Si falla, dejar el valor original
          }
        }
      });

      // Asegurar que el campo DIAS_SIN_VENIR sea un número
      if (processedRow.DIAS_SIN_VENIR) {
        if (typeof processedRow.DIAS_SIN_VENIR === 'string') {
          const parsedValue = parseInt(processedRow.DIAS_SIN_VENIR, 10);
          if (!isNaN(parsedValue)) {
            processedRow.DIAS_SIN_VENIR = parsedValue;
          }
        }
      } else {
        // Si no existe, intentar calcularlo a partir de ULT_VISITA
        if (processedRow.ULT_VISITA) {
          try {
            // Modificado para trabajar con el nuevo formato DD/MM/YYYY
            const fechaParts = processedRow.ULT_VISITA.split('/');
            if (fechaParts.length === 3) {
              const dia = parseInt(fechaParts[0], 10);
              const mes = parseInt(fechaParts[1], 10) - 1;
              const año = parseInt(fechaParts[2], 10);

              if (dia > 0 && mes >= 0 && año > 0) {
                const fechaUltVisita = new Date(año, mes, dia);
                const fechaActual = new Date();
                const diffTime = Math.abs(fechaActual - fechaUltVisita);
                const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                processedRow.DIAS_SIN_VENIR = diffDays;
              }
            }
          } catch (e) {
            console.warn(`No se pudo calcular DIAS_SIN_VENIR para un registro: ${e.message}`);
          }
        }
      }

      return processedRow;
    });

    // Guardar los datos en formato CSV
    const result = await saveToCSVWithRetry(config.fileName, processedData, config.encoding);

    // Después de guardar el archivo, verificar su tamaño y dividirlo si es necesario
    const csvPath = path.join(__dirname, '../../public', config.fileName);
    if (fs.existsSync(csvPath)) {
      const fileStats = fs.statSync(csvPath);
      const fileSizeMB = fileStats.size / (1024 * 1024);

      // Si el archivo es mayor a 50MB, dividirlo
      if (fileSizeMB > 50) {
        console.log(`El archivo ${config.fileName} es grande (${fileSizeMB.toFixed(2)}MB). Dividiéndolo...`);
        const baseName = path.basename(config.fileName, path.extname(config.fileName));
        const outputPrefix = path.join(__dirname, '../../public', baseName);

        try {
          await splitCSVFile(csvPath, outputPrefix);
          console.log(`Archivo dividido en fragmentos: ${outputPrefix}_N.csv`);

          // Comprimir los fragmentos
          const fragmentDir = path.dirname(outputPrefix);
          const fragmentBaseName = path.basename(outputPrefix);
          const fragmentFiles = fs.readdirSync(fragmentDir)
            .filter(file => file.startsWith(fragmentBaseName) && file.endsWith('.csv'));

          for (const fragmentFile of fragmentFiles) {
            const fragmentPath = path.join(fragmentDir, fragmentFile);
            try {
              const compressedPath = `${fragmentPath}.gz`;
              await compressCSV(fragmentPath, compressedPath);
              console.log(`✅ Fragmento comprimido: ${compressedPath}`);
            } catch (error) {
              console.error(`⚠️ Error al comprimir fragmento ${fragmentFile}: ${error.message}`);
            }
          }
        } catch (splitError) {
          console.error(`Error al dividir el archivo: ${splitError.message}`);
        }
      } else {
        // Si el archivo no es muy grande, simplemente comprimirlo
        try {
          const compressedPath = `${csvPath}.gz`;
          await compressCSV(csvPath, compressedPath);
          console.log(`✅ Archivo comprimido: ${compressedPath}`);
        } catch (compressError) {
          console.error(`⚠️ Error al comprimir el archivo: ${compressError.message}`);
        }
      }
    }

    return {
      agency: agencyName,
      ...result,
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    console.error(`Error al actualizar CSV para ${agencyName}:`, error);

    // Verificar si ya existe un archivo para esta agencia
    const csvPath = path.join(__dirname, '../../public', agencyConfig[agencyName].fileName);
    if (fs.existsSync(csvPath)) {
      console.log(`✅ Se encontró un archivo CSV existente para ${agencyName}: ${csvPath}`);
      console.log(`   Se mantiene el archivo existente debido al error: ${error.message}`);
      return {
        agency: agencyName,
        success: true,
        message: `Se mantuvo el archivo existente. Error original: ${error.message}`,
        filePath: csvPath,
        timestamp: new Date().toISOString()
      };
    }

    return {
      agency: agencyName,
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

/**
 * Actualiza los CSV para todas las agencias
 * @returns {Promise<Object>} - Resultados de todas las operaciones
 */
async function updateAllCSVs() {
  // Limpiamos la carpeta Public antes de comenzar
  await limpiarCarpetaPublic();

  const agencies = Object.keys(agencyConfig);
  const results = {};

  // En la función updateAllCSVs, modifica el bucle for:
  for (const agency of agencies) {
    console.log(`Procesando agencia: ${agency}`);
    try {
      results[agency] = await updateAgencyCSV(agency);

      // Añadir una pausa de 2 segundos entre cada agencia para liberar recursos
      console.log(`Esperando 2 segundos antes de procesar la siguiente agencia...`);
      await new Promise(resolve => setTimeout(resolve, 2000));

    } catch (error) {
      console.error(`Error no controlado para ${agency}:`, error);
      results[agency] = {
        agency,
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  // Resumen de operaciones
  const summary = {
    timestamp: new Date().toISOString(),
    totalAgencies: agencies.length,
    successCount: Object.values(results).filter(r => r.success).length,
    failureCount: Object.values(results).filter(r => !r.success).length,
    details: results
  };

  // Guardar log de resultados
  const logFile = path.join(LOGS_DIR, `csv-update-ret-${new Date().toISOString().split('T')[0]}.json`);
  fs.writeFileSync(logFile, JSON.stringify(summary, null, 2));

  return summary;
}

/**
 * Actualiza solo una agencia específica
 * @param {string} agencyName - Nombre de la agencia a actualizar
 * @returns {Promise<Object>} - Resultado de la actualización
 */
async function updateSingleAgency(agencyName) {
  if (!agencyConfig[agencyName]) {
    throw new Error(`Agencia no configurada: ${agencyName}`);
  }

  console.log(`Actualizando exclusivamente la agencia: ${agencyName}`);

  try {
    const result = await updateAgencyCSV(agencyName);

    // Guardar log del resultado
    const logFile = path.join(LOGS_DIR, `csv-update-${agencyName}-${new Date().toISOString().split('T')[0]}.json`);
    fs.writeFileSync(logFile, JSON.stringify(result, null, 2));

    return result;
  } catch (error) {
    console.error(`Error al actualizar la agencia ${agencyName}:`, error);
    throw error;
  }
}

// Manejar ejecución directa con argumentos
const agencyArg = process.argv[2];
const allArg = process.argv.includes('--all');

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  if (allArg) {
    console.log('Ejecutando actualización de CSV de retención para todas las agencias...');
    updateAllCSVs()
      .then(results => {
        console.log('Resultados de actualización:');
        console.log(JSON.stringify(results, null, 2));
        process.exit(0);
      })
      .catch(error => {
        console.error('Error en actualización:', error);
        process.exit(1);
      });
  } else if (agencyArg && agencyConfig[agencyArg]) {
    console.log(`Ejecutando actualización para agencia específica: ${agencyArg}`);
    updateSingleAgency(agencyArg)
      .then(result => {
        console.log(`Resultado para ${agencyArg}:`);
        console.log(JSON.stringify(result, null, 2));
        process.exit(0);
      })
      .catch(error => {
        console.error(`Error en actualización de ${agencyArg}:`, error);
        process.exit(1);
      });
  } else if (agencyArg) {
    console.error(`❌ Agencia no válida: ${agencyArg}`);
    console.log(`Agencias disponibles: ${Object.keys(agencyConfig).join(', ')}`);
    process.exit(1);
  } else {
    console.log('Uso: node updateCSV_retencion.js [agencia] | --all');
    console.log(`Agencias disponibles: ${Object.keys(agencyConfig).join(', ')}`);
    process.exit(0);
  }
}

// Exportar las funciones
export { updateAgencyCSV, updateAllCSVs, updateSingleAgency };