// service/UserService.ts
// Servicio para gestionar usuarios - conecta con el backend

export interface UserData {
  id: number;
  password?: string;
  is_superuser: boolean;
  first_name: string;
  last_name: string;
  email: string;
  Agencia: string | null;
}

const API_URL = 'https://lokerserver-production.up.railway.app';

/**
 * Obtiene todos los usuarios del sistema
 */
export const getAllUsers = async (): Promise<UserData[]> => {
  try {
    console.log('Solicitando usuarios desde:', `${API_URL}/users`);
    const response = await fetch(`${API_URL}/users`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        // Asegurar que Accept está definido correctamente
        'Accept': 'application/json'
      },
      // Asegurar que las credenciales no causan problemas de CORS
      credentials: 'omit'
    });

    console.log('Respuesta del servidor:', response.status, response.statusText);
    
    if (!response.ok) {
      let errorMessage = 'Error al obtener usuarios';
      try {
        const errorData = await response.json();
        errorMessage = errorData.message || errorMessage;
      } catch (e) {
        console.warn('No se pudo parsear la respuesta de error', e);
      }
      throw new Error(errorMessage);
    }

    const data = await response.json();
    console.log('Datos recibidos:', data);
    
    if (!data.users) {
      console.warn('La respuesta no contiene la propiedad "users":', data);
      // Si el servidor no devuelve {users: [...]} sino directamente un array
      return Array.isArray(data) ? data : [];
    }
    
    return data.users;
  } catch (error) {
    console.error('Error en getAllUsers:', error);
    throw error;
  }
};

/**
 * Actualiza un usuario existente
 */
export const updateUser = async (userId: number, userData: Partial<UserData>): Promise<UserData> => {
  try {
    console.log(`Actualizando usuario ${userId} con datos:`, userData);
    const response = await fetch(`${API_URL}/users/${userId}`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      credentials: 'omit',
      body: JSON.stringify(userData)
    });

    console.log('Respuesta del servidor:', response.status, response.statusText);
    
    if (!response.ok) {
      let errorMessage = 'Error al actualizar usuario';
      try {
        const errorData = await response.json();
        errorMessage = errorData.message || errorMessage;
      } catch (e) {
        console.warn('No se pudo parsear la respuesta de error', e);
      }
      throw new Error(errorMessage);
    }

    const data = await response.json();
    console.log('Datos de usuario actualizado:', data);
    
    if (!data.user) {
      console.warn('La respuesta no contiene la propiedad "user":', data);
      // Si el servidor no devuelve {user: {...}} sino directamente el objeto
      return data;
    }
    
    return data.user;
  } catch (error) {
    console.error('Error en updateUser:', error);
    throw error;
  }
};

/**
 * Crea un nuevo usuario
 */
export const createUser = async (userData: Omit<UserData, 'id'>): Promise<UserData> => {
  try {
    console.log('Creando nuevo usuario con datos:', userData);
    const response = await fetch(`${API_URL}/users`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      credentials: 'omit',
      body: JSON.stringify(userData)
    });

    console.log('Respuesta del servidor:', response.status, response.statusText);
    
    if (!response.ok) {
      let errorMessage = 'Error al crear usuario';
      try {
        const errorData = await response.json();
        errorMessage = errorData.message || errorMessage;
      } catch (e) {
        console.warn('No se pudo parsear la respuesta de error', e);
      }
      throw new Error(errorMessage);
    }

    const data = await response.json();
    console.log('Datos de usuario creado:', data);
    
    if (!data.user) {
      console.warn('La respuesta no contiene la propiedad "user":', data);
      // Si el servidor no devuelve {user: {...}} sino directamente el objeto
      return data;
    }
    
    return data.user;
  } catch (error) {
    console.error('Error en createUser:', error);
    throw error;
  }
};

/**
 * Elimina un usuario
 */
export const deleteUser = async (userId: number): Promise<{ success: boolean }> => {
  try {
    console.log(`Eliminando usuario ${userId}`);
    const response = await fetch(`${API_URL}/users/${userId}`, {
      method: 'DELETE',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      credentials: 'omit'
    });

    console.log('Respuesta del servidor:', response.status, response.statusText);
    
    if (!response.ok) {
      let errorMessage = 'Error al eliminar usuario';
      try {
        const errorData = await response.json();
        errorMessage = errorData.message || errorMessage;
      } catch (e) {
        console.warn('No se pudo parsear la respuesta de error', e);
      }
      throw new Error(errorMessage);
    }

    const data = await response.json();
    console.log('Respuesta de eliminación:', data);
    
    return { success: true };
  } catch (error) {
    console.error('Error en deleteUser:', error);
    throw error;
  }
};