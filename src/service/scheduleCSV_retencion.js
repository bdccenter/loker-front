// src/service/scheduleCSV_retencion.js
import cron from 'node-cron';
import { updateAllCSVs } from './updateCSV_retencion.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Obtener la ruta del archivo actual
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Definir rutas para logs
const LOGS_DIR = path.join(__dirname, '../../logs');
const LOG_FILE = path.join(LOGS_DIR, 'csv-schedule-retencion.log');

// Asegurar que existe el directorio de logs
if (!fs.existsSync(LOGS_DIR)) {
  fs.mkdirSync(LOGS_DIR, { recursive: true });
}

/**
 * Agrega un mensaje al archivo de log con formato de fecha personalizado
 * @param {string} message - Mensaje a registrar
 */
function logMessage(message) {
  // Obtener la fecha actual
  const now = new Date();

  // Formatear la fecha en formato DD/MM/YYYY HH:MM:SS
  const day = now.getDate().toString().padStart(2, '0');
  const month = (now.getMonth() + 1).toString().padStart(2, '0');
  const year = now.getFullYear();
  const hours = now.getHours().toString().padStart(2, '0');
  const minutes = now.getMinutes().toString().padStart(2, '0');
  const seconds = now.getSeconds().toString().padStart(2, '0');

  const formattedDate = `${day}/${month}/${year} ${hours}:${minutes}:${seconds}`;

  // Crear la entrada de log con el nuevo formato
  const logEntry = `[${formattedDate}] ${message}\n`;

  try {
    fs.appendFileSync(LOG_FILE, logEntry);
  } catch (error) {
    console.error('Error al escribir en el log:', error);
  }

  // También mostrar en consola para que aparezca en los logs de Railway
  console.log(`${formattedDate} - ${message}`);
}

/**
 * Realiza la actualización de todos los CSV de retención
 * @param {string} scheduleName - Nombre de la programación para logging
 */
async function performUpdate(scheduleName = 'programada') {
  logMessage(`Iniciando actualización ${scheduleName} de archivos CSV de retención desde BigQuery`);

  try {
    const results = await updateAllCSVs();

    // Resumen de la operación
    const successCount = Object.values(results.details).filter(r => r.success).length;
    const failureCount = Object.values(results.details).filter(r => !r.success).length;

    if (successCount === results.totalAgencies) {
      logMessage(`✅ Actualización ${scheduleName} exitosa para todas las agencias (${successCount}/${results.totalAgencies})`);
    } else {
      logMessage(`⚠️ Actualización ${scheduleName} completada con advertencias: ${successCount} exitosas, ${failureCount} fallidas`);

      // Registrar detalles de fallos
      Object.values(results.details)
        .filter(r => !r.success)
        .forEach(r => {
          logMessage(`  - Error en ${r.agency}: ${r.error}`);
        });
    }

    return results;
  } catch (error) {
    logMessage(`❌ Error crítico en actualización ${scheduleName}: ${error.message}`);
    console.error(error);
    throw error;
  }
}

// Configuración de zona horaria común para todas las programaciones
const cronOptions = {
  scheduled: true,
  timezone: "America/Hermosillo" // Para Sonora, que no usa horario de verano
};

// Programar la tarea para ejecutarse a las 10:00 AM
// Formato cron: minuto hora dia-mes mes dia-semana
cron.schedule('0 10 * * *', () => performUpdate('matutina (10:00 AM)'), cronOptions);

// Programar la tarea para ejecutarse a la 1:00 PM
cron.schedule('0 13 * * *', () => performUpdate('vespertina (1:00 PM)'), cronOptions);

logMessage('🚀 Servicio de actualización de CSV de retención iniciado');
logMessage('📅 Programado para ejecutarse dos veces al día:');
logMessage('   - 10:00 AM (actualización matutina)');
logMessage('   - 1:00 PM (actualización vespertina)');

// Ejecutar una actualización inmediata al iniciar el servicio
logMessage('⏱️ Ejecutando una actualización inicial inmediata...');
performUpdate('inicial')
  .then(results => {
    logMessage('✅ Actualización inicial completada con éxito.');
    logMessage(`   - Total agencias: ${results.totalAgencies}`);
    logMessage(`   - Actualizadas con éxito: ${results.successCount}`);
    logMessage(`   - Fallidas: ${results.failureCount}`);
    logMessage('El servicio continuará ejecutándose según la programación establecida.');
  })
  // Si hay un error en la actualización inicial, se maneja aquí
  .catch(error => {
    logMessage(`❌ Error en actualización inicial: ${error.message}`);
    logMessage('El servicio continuará intentando actualizaciones según la programación establecida.');
  });

// Para mantener el proceso activo
process.stdin.resume();

// Manejar señales de terminación
process.on('SIGINT', () => {
  logMessage('Servicio de actualización de CSV de retención detenido por el usuario');
  process.exit(0);
});

// Manejar señales de terminación
process.on('SIGTERM', () => {
  logMessage('Servicio de actualización de CSV de retención detenido por el sistema');
  process.exit(0);
});

export { performUpdate };