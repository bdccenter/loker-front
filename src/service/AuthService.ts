export interface User {
  id: number;
  firstName: string;
  lastName: string;
  email: string;
  isSuperuser: boolean;
  agencia?: string | null;
}

const USER_KEY = 'autoinsights_user';
const SESSION_TIMESTAMP_KEY = 'autoinsights_session_timestamp';
const SESSION_TIMEOUT = 15 * 60 * 15000; 
const API_URL = 'https://lokerserver-production.up.railway.app';

/**
 * Inicia sesión con las credenciales proporcionadas
 */
export const login = async (email: string, password: string): Promise<User> => {
  try {
    console.log(`Intentando login con email: ${email}`);
    
    const response = await fetch(`${API_URL}/login`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ email, password }),
    });

    const data = await response.json();

    if (!response.ok) {
      console.error('Error en respuesta:', data);
      throw new Error(data.message || 'Error al iniciar sesión');
    }

    // Guardar información del usuario en localStorage
    localStorage.setItem(USER_KEY, JSON.stringify(data.user));
    
    // Inicializar el timestamp de la sesión
    updateSessionTimestamp();

    console.log('Login exitoso:', data.user);
    return data.user;
  } catch (error) {
    console.error('Error en inicio de sesión:', error);
    throw error;
  }
};

/**
 * Cierra la sesión del usuario actual
 */
export const logout = (): void => {
  localStorage.removeItem(USER_KEY);
  localStorage.removeItem(SESSION_TIMESTAMP_KEY);
  // Redirigir a la página de inicio de sesión se maneja en el componente
};

/**
 * Actualiza el timestamp de la última actividad de la sesión
 */
export const updateSessionTimestamp = (): void => {
  localStorage.setItem(SESSION_TIMESTAMP_KEY, Date.now().toString());
};

/**
 * Comprueba si la sesión ha expirado por inactividad
 */
export const hasSessionExpired = (): boolean => {
  const timestampStr = localStorage.getItem(SESSION_TIMESTAMP_KEY);
  
  // Si no hay timestamp, la sesión ha expirado
  if (!timestampStr) return true;
  
  const timestamp = parseInt(timestampStr, 10);
  const now = Date.now();
  
  // Verificar si han pasado más de 15 minutos desde la última actividad
  return now - timestamp > SESSION_TIMEOUT;
};

/**
 * Verifica si hay un usuario autenticado y que la sesión no haya expirado
 */
export const isAuthenticated = (): boolean => {
  const user = localStorage.getItem(USER_KEY);
  
  // Verificar si hay un usuario en localStorage
  if (!user) return false;
  
  // Verificar si la sesión ha expirado
  if (hasSessionExpired()) {
    // Si la sesión expiró, hacer logout automático
    logout();
    return false;
  }
  
  return true;
};

/**
 * Obtiene la información del usuario autenticado
 */
export const getCurrentUser = (): User | null => {
  try {
    // Verificar primero si la sesión ha expirado
    if (hasSessionExpired()) {
      logout();
      return null;
    }
    
    const userJson = localStorage.getItem(USER_KEY);
    if (userJson) {
      return JSON.parse(userJson);
    }
    return null;
  } catch (error) {
    console.error('Error al obtener usuario actual:', error);
    logout(); // Si hay error al leer el usuario, cerrar sesión
    return null;
  }
};

/**
 * Verifica si el usuario tiene acceso a una agencia específica
 */
export const canAccessAgencia = (agenciaNombre: string): boolean => {
  const currentUser = getCurrentUser();
  
  if (!currentUser) return false;
  
  // Si es superusuario o no tiene agencia asignada (NULL en la DB), puede acceder a cualquier agencia
  if (currentUser.isSuperuser || currentUser.agencia === null) {
    return true;
  }
  
  // Si no es superusuario, solo puede acceder a su agencia asignada
  return currentUser.agencia === agenciaNombre;
};

/**
 * Obtiene las agencias a las que el usuario tiene acceso
 */
export const getAccessibleAgencias = (): string[] => {
  const currentUser = getCurrentUser();
  
  if (!currentUser) return [];
  
  // Si es superusuario o no tiene agencia asignada (NULL en la DB), puede acceder a todas
  if (currentUser.isSuperuser || currentUser.agencia === null) {
    return ['Gran Auto', 'Gasme', 'Sierra', 'Huerpel', 'Del Bravo'];
  }
  
  // Si no es superusuario, solo puede acceder a su agencia asignada
  return currentUser.agencia ? [currentUser.agencia] : [];
};