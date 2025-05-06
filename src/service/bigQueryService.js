// src/services/bigQueryService.js - ACTUALIZADO
import { BigQuery } from '@google-cloud/bigquery';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Obtener la ruta del archivo actual
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Ruta a las credenciales de BigQuery
const CREDENTIALS_PATH = path.join(__dirname, '../../credentials/bigquery-key.json');
// Ruta a la carpeta public donde se guardarán los archivos CSV
const PUBLIC_PATH = path.join(__dirname, '../../public');

// Verificar que el archivo de credenciales existe
if (!fs.existsSync(CREDENTIALS_PATH)) {
  console.error(`❌ ERROR: Archivo de credenciales no encontrado en: ${CREDENTIALS_PATH}`);
  console.error(`Ruta completa: ${path.resolve(CREDENTIALS_PATH)}`);
  console.error('Por favor, asegúrate de que existe el archivo de credenciales.');
}

// Asegurar que existe el directorio public
if (!fs.existsSync(PUBLIC_PATH)) {
  fs.mkdirSync(PUBLIC_PATH, { recursive: true });
  console.log(`✅ Directorio public creado en: ${PUBLIC_PATH}`);
} else {
  console.log(`✅ Directorio public existente en: ${PUBLIC_PATH}`);
}

// Instanciar el cliente de BigQuery con manejo de errores
let bigquery;
try {
  bigquery = new BigQuery({
    projectId: 'base-maestra-gn',
    keyFilename: CREDENTIALS_PATH
  });
  console.log('✅ Cliente BigQuery inicializado correctamente');
} catch (error) {
  console.error('❌ ERROR: No se pudo inicializar el cliente de BigQuery:', error.message);
}

/**
 * Guarda datos en un archivo CSV con reintentos automáticos
 * @param {string} filename - Nombre del archivo
 * @param {Array} data - Datos a guardar
 * @param {string} encoding - Codificación del archivo (default: utf8)
 * @param {number} maxRetries - Número máximo de reintentos (default: 3)
 * @param {number} delay - Tiempo entre reintentos en ms (default: 1500)
 * @returns {Promise<Object>} - Resultado de la operación
 */
async function saveToCSVWithRetry(filename, data, encoding = 'utf8', maxRetries = 3, delay = 1500) {
  let attempt = 0;
  let lastError = null;

  while (attempt < maxRetries) {
    try {
      attempt++;

      // Intentar guardar el archivo
      const csvPath = path.join(PUBLIC_PATH, filename);
      const csvContent = convertToCSV(data);

      // Verificar que el directorio existe
      if (!fs.existsSync(PUBLIC_PATH)) {
        console.log(`Creando directorio: ${PUBLIC_PATH}`);
        fs.mkdirSync(PUBLIC_PATH, { recursive: true });
      }

      // Intentar usar un nombre temporal primero
      const tempFilename = `temp_${Date.now()}_${filename}`;
      const tempPath = path.join(PUBLIC_PATH, tempFilename);

      // Guardar primero en archivo temporal
      fs.writeFileSync(tempPath, csvContent, encoding);

      // Si el archivo de destino existe, intentar eliminarlo
      if (fs.existsSync(csvPath)) {
        try {
          fs.unlinkSync(csvPath);
        } catch (unlinkErr) {
          console.warn(`No se pudo eliminar el archivo existente: ${unlinkErr.message}`);
          // Continuar de todos modos e intentar renombrar
        }
      }

      // Renombrar el archivo temporal al nombre final
      fs.renameSync(tempPath, csvPath);

      console.log(`✅ Archivo CSV guardado en: ${csvPath}`);
      console.log(`   - ${data.length} registros`);
      console.log(`   - Tamaño: ${(csvContent.length / 1024).toFixed(2)} KB`);
      console.log(`   - Codificación: ${encoding}`);

      return {
        success: true,
        recordCount: data.length,
        filePath: csvPath,
        fileSize: csvContent.length
      };
    } catch (error) {
      lastError = error;

      // Si es error EBUSY y todavía hay reintentos disponibles
      if (error.code === 'EBUSY' && attempt < maxRetries) {
        console.log(`⚠️ Archivo ${filename} ocupado. Reintentando en ${delay}ms (intento ${attempt}/${maxRetries})...`);

        // Esperar antes de reintentar
        await new Promise(resolve => setTimeout(resolve, delay));
      } else {
        // Si es otro tipo de error o ya no hay más reintentos, lanzar el error
        console.error(`❌ Error al guardar CSV ${filename} después de ${attempt} intentos:`, error);
        return {
          success: false,
          error: error.message
        };
      }
    }
  }

  // Si llegamos aquí, todos los reintentos fallaron
  return {
    success: false,
    error: lastError ? lastError.message : 'Error desconocido después de múltiples intentos'
  };
}

async function executeQuery(projectId, query) {
  try {
    // Verificar que el archivo de credenciales existe antes de ejecutar la consulta
    if (!fs.existsSync(CREDENTIALS_PATH)) {
      throw new Error(`Archivo de credenciales no encontrado en: ${CREDENTIALS_PATH}`);
    }

    console.log(`Ejecutando consulta en proyecto: ${projectId}`);
    console.log(`Consulta a ejecutar:\n${query}`);

    // Si es necesario cambiar el proyecto
    if (projectId !== 'base-maestra-gn') {
      console.log(`Cambiando a proyecto: ${projectId}`);

      const tempBigQuery = new BigQuery({
        projectId: projectId,
        keyFilename: CREDENTIALS_PATH
      });

      const [rows] = await tempBigQuery.query({ query });
      console.log(`Consulta exitosa en ${projectId}: ${rows.length} filas obtenidas`);
      return rows;
    } else {
      // Usar la instancia por defecto
      console.log('Usando instancia por defecto de BigQuery');
      const [rows] = await bigquery.query({ query });
      console.log(`Consulta exitosa en ${projectId}: ${rows.length} filas obtenidas`);
      return rows;
    }
  } catch (error) {
    console.error(`Error al ejecutar consulta en ${projectId}:`, error);

    // Proporcionar información más detallada sobre el error
    if (error.code === 403) {
      console.error(`⚠️ Error de permisos: Verifica que la cuenta de servicio tiene los permisos necesarios en el proyecto ${projectId}`);
    } else if (error.code === 400) {
      console.error('⚠️ Error en la consulta SQL: Verifica los nombres de las columnas y la sintaxis');

      // Extraer el mensaje de error específico si está disponible
      if (error.errors && error.errors.length > 0) {
        console.error(`Detalles del error: ${error.errors[0].message}`);
      }
    }

    throw error;
  }
}

// Función para agregar a bigQueryService.js
async function exploreTables(projectId, datasetName, tableName) {
  try {
    // Consulta para obtener valores únicos de AGENCI o AGENCIA
    const query = `
    SELECT DISTINCT 
      CASE 
        WHEN AGENCI IS NOT NULL THEN AGENCI 
        WHEN AGENCIA IS NOT NULL THEN AGENCIA 
        ELSE NULL 
      END as AgencyName
    FROM \`${projectId}.${datasetName}.${tableName}\`
    WHERE AGENCI IS NOT NULL OR AGENCIA IS NOT NULL
    ORDER BY AgencyName`;
    
    const results = await executeQuery(projectId, query);
    console.log('Agencias disponibles:', results.map(r => r.AgencyName));
    return results.map(r => r.AgencyName);
  } catch (error) {
    console.error(`Error al explorar agencias:`, error);
    throw error;
  }
}

/**
 * Formatea un valor para CSV, con manejo especial para fechas
 * @param {any} value - Valor a formatear
 * @returns {string} - Valor formateado
 */
function formatCSVValue(value) {
  if (value === null || value === undefined) {
    return '';
  }

  // Manejo especial para objetos Date
  if (value instanceof Date) {
    try {
      // Formatear como DD MMM YYYY en español
      const day = value.getDate().toString().padStart(2, '0');
      const month = value.toLocaleString('es-ES', { month: 'short' });
      const year = value.getFullYear();
      return `"${day} ${month} ${year}"`;
    } catch (error) {
      console.error('Error al formatear fecha:', error);
      return '""';
    }
  }

  // Manejar objetos que podrían ser fechas pero no son instancias de Date
  if (typeof value === 'object' && value !== null) {
    try {
      // Si es un objeto con propiedades value o _value, intentar extraer
      if (value.value !== undefined) {
        return formatCSVValue(value.value);
      }

      // Intentar convertir a string y ver si es un formato de fecha ISO
      const valueStr = String(value);
      if (/^\d{4}-\d{2}-\d{2}/.test(valueStr)) {
        try {
          const date = new Date(valueStr);
          return formatCSVValue(date);
        } catch (e) {
          // Si falla, devolver el string original
          return `"${valueStr.replace(/"/g, '""')}"`;
        }
      }

      // Si todo falla, convertir a JSON
      return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
    } catch (error) {
      console.error('Error al procesar objeto:', error);
      return '""';
    }
  }

  // Convertir a string y escapar comillas dobles
  return `"${String(value).replace(/"/g, '""')}"`;
}

/**
 * Convierte un array de objetos a formato CSV con mejor manejo de valores
 * @param {Array} data - Datos a convertir
 * @returns {string} - Contenido en formato CSV
 */
function convertToCSV(data) {
  if (!data || data.length === 0) {
    console.warn('No hay datos para convertir a CSV');
    return '';
  }

  // Obtener encabezados
  const headers = Object.keys(data[0]);
  console.log(`Encabezados detectados: ${headers.join(', ')}`);

  // Crear línea de encabezados
  const csvRows = [headers.join(',')];

  // Agregar datos
  for (const row of data) {
    const values = headers.map(header => {
      return formatCSVValue(row[header]);
    });
    csvRows.push(values.join(','));
  }

  return csvRows.join('\n');
}

/**
 * Guarda datos en un archivo CSV con manejo de codificación específica
 * @param {string} filename - Nombre del archivo
 * @param {Array} data - Datos a guardar
 * @param {string} encoding - Codificación del archivo (default: utf8)
 * @returns {Promise<Object>} - Resultado de la operación
 */
async function saveToCSV(filename, data, encoding = 'utf8') {
  try {
    const csvPath = path.join(PUBLIC_PATH, filename);
    const csvContent = convertToCSV(data);

    // Verificar que el directorio existe
    if (!fs.existsSync(PUBLIC_PATH)) {
      console.log(`Creando directorio: ${PUBLIC_PATH}`);
      fs.mkdirSync(PUBLIC_PATH, { recursive: true });
    }

    fs.writeFileSync(csvPath, csvContent, encoding);

    console.log(`✅ Archivo CSV guardado en: ${csvPath}`);
    console.log(`   - ${data.length} registros`);
    console.log(`   - Tamaño: ${(csvContent.length / 1024).toFixed(2)} KB`);
    console.log(`   - Codificación: ${encoding}`);

    return {
      success: true,
      recordCount: data.length,
      filePath: csvPath,
      fileSize: csvContent.length
    };
  } catch (error) {
    console.error(`❌ Error al guardar CSV ${filename}:`, error);
    return {
      success: false,
      error: error.message
    };
  }
}

// Si se ejecuta directamente este archivo
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  console.log('Verificando configuración de BigQuery...');
  console.log(`Ruta de credenciales: ${path.resolve(CREDENTIALS_PATH)}`);
  console.log(`Directorio public: ${path.resolve(PUBLIC_PATH)}`);

  if (fs.existsSync(CREDENTIALS_PATH)) {
    console.log('✅ Archivo de credenciales encontrado');

    // Mostrar información básica del archivo (sin imprimir el contenido sensible)
    const stats = fs.statSync(CREDENTIALS_PATH);
    console.log(`   - Tamaño: ${(stats.size / 1024).toFixed(2)} KB`);
    console.log(`   - Última modificación: ${stats.mtime}`);
  } else {
    console.error('❌ Archivo de credenciales NO encontrado');
  }
}

export { executeQuery, saveToCSV, saveToCSVWithRetry };