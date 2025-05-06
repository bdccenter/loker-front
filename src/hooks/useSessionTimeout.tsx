import { useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { updateSessionTimestamp, isAuthenticated } from '../service/AuthService';

/**
 * Hook personalizado para manejar el timeout de sesión por inactividad
 * @param timeoutInterval Intervalo en milisegundos para verificar si la sesión ha expirado (default: 1 minuto)
 */
export const useSessionTimeout = (timeoutInterval: number = 60000) => {
  const navigate = useNavigate();
  
  // Actualizar el timestamp cada vez que el usuario interactúe con la aplicación
  const updateActivity = useCallback(() => {
    // Solo actualizar si el usuario está autenticado
    if (isAuthenticated()) {
      updateSessionTimestamp();
    }
  }, []);
  
  // Verificar periódicamente si la sesión ha expirado
  useEffect(() => {
    // Solo iniciar el intervalo si el usuario está autenticado
    if (!isAuthenticated()) return;
    
    // Verificar cada minuto si la sesión ha expirado
    const intervalId = setInterval(() => {
      if (!isAuthenticated()) {
        // Si la sesión ha expirado (se hizo logout internamente en isAuthenticated),
        // redirigir a la página de login
        navigate('/login', { replace: true });
      }
    }, timeoutInterval);
    
    // Limpiar el intervalo cuando el componente se desmonte
    return () => clearInterval(intervalId);
  }, [navigate, timeoutInterval]);
  
  // Configurar listeners para eventos de actividad del usuario
  useEffect(() => {
    const events = ['mousedown', 'mousemove', 'keypress', 'scroll', 'touchstart'];
    
    // Registrar todos los eventos
    events.forEach(event => {
      window.addEventListener(event, updateActivity);
    });
    
    // Eliminar los listeners cuando el componente se desmonte
    return () => {
      events.forEach(event => {
        window.removeEventListener(event, updateActivity);
      });
    };
  }, [updateActivity]);
  
  // Exponer función para verificar manualmente si la sesión ha expirado
  const checkSession = useCallback(() => {
    if (!isAuthenticated()) {
      navigate('/login', { replace: true });
      return false;
    }
    return true;
  }, [navigate]);
  
  return { checkSession };
};

export default useSessionTimeout;