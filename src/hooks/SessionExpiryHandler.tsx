import React, { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { hasSessionExpired, isAuthenticated } from '../service/AuthService';
import useSessionTimeout from '../hooks/useSessionTimeout';

/**
 * Componente que detecta si la sesión ha expirado y redirige al usuario
 * a la página de login con un parámetro indicando que la sesión expiró.
 */
const SessionExpiryHandler: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const navigate = useNavigate();
  
  // Usar el hook de timeout de sesión
  useSessionTimeout();
  
  useEffect(() => {
    // Comprobar si la sesión ha expirado al cargar la página
    if (hasSessionExpired() && !isAuthenticated()) {
      navigate('/login?sessionExpired=true', { replace: true });
    }
  }, [navigate]);
  
  return <>{children}</>;
};

export default SessionExpiryHandler;