// Servicio para gestionar el historial de búsquedas en localStorage

const HISTORIAL_KEY = 'nissan_app_search_history';
const MAX_HISTORIAL_ITEMS = 10;

/**
 * Obtiene el historial de búsquedas del localStorage
 */
export const obtenerHistorialBusquedas = (): string[] => {
  try {
    const historialJSON = localStorage.getItem(HISTORIAL_KEY);
    if (historialJSON) {
      return JSON.parse(historialJSON);
    }
  } catch (error) {
    console.error('Error al obtener historial de búsquedas:', error);
  }
  return [];
};

/**
 * Guarda un término en el historial de búsquedas
 */

export const guardarEnHistorial = (termino: string): string[] => {
  try {
    if (!termino.trim()) return obtenerHistorialBusquedas();
    
    // Obtener historial actual
    const historialActual = obtenerHistorialBusquedas();
    
    // Verificar si el término ya existe
    const indiceExistente = historialActual.indexOf(termino);
    if (indiceExistente !== -1) {
      // Si existe, eliminarlo para ponerlo al principio (más reciente)
      historialActual.splice(indiceExistente, 1);
    }
    
    // Añadir al principio
    historialActual.unshift(termino);
    
    // Limitar a MAX_HISTORIAL_ITEMS elementos
    const nuevoHistorial = historialActual.slice(0, MAX_HISTORIAL_ITEMS);
    
    // Guardar en localStorage
    localStorage.setItem(HISTORIAL_KEY, JSON.stringify(nuevoHistorial));
    
    return nuevoHistorial;
  } catch (error) {
    console.error('Error al guardar en historial de búsquedas:', error);
    return obtenerHistorialBusquedas();
  }
};

/**
 * Limpia todo el historial de búsquedas
 */
export const limpiarHistorialBusquedas = (): void => {
  try {
    localStorage.removeItem(HISTORIAL_KEY);
  } catch (error) {
    console.error('Error al limpiar historial de búsquedas:', error);
  }
};

/**
 * Elimina un término específico del historial
 */
export const eliminarDelHistorial = (termino: string): string[] => {
  try {
    const historialActual = obtenerHistorialBusquedas();
    const nuevoHistorial = historialActual.filter(item => item !== termino);
    localStorage.setItem(HISTORIAL_KEY, JSON.stringify(nuevoHistorial));
    return nuevoHistorial;
  } catch (error) {
    console.error('Error al eliminar del historial de búsquedas:', error);
    return obtenerHistorialBusquedas();
  }
};