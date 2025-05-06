import React, { useState, useEffect } from 'react';
import { X } from 'lucide-react';
import { getAllUsers, updateUser, createUser, deleteUser, UserData } from '../service/UserService';

// Props para el componente AdminPanel
interface AdminPanelProps {
  isOpen: boolean;
  onClose: () => void;
}

const AdminPanel: React.FC<AdminPanelProps> = ({ isOpen, onClose }) => {
  // Estado para almacenar los usuarios
  const [users, setUsers] = useState<UserData[]>([]);
  // Estado para el usuario que se está editando
  const [editingUser, setEditingUser] = useState<UserData | null>(null);
  // Estado para mostrar/ocultar el modal de edición
  const [showEditModal, setShowEditModal] = useState<boolean>(false);
  // Estado específico para el campo de contraseña en la edición
  const [editPassword, setEditPassword] = useState('');
  // Estado para el nuevo usuario que se está creando
  const [newUser, setNewUser] = useState<Partial<UserData>>({
    password: '',
    is_superuser: false,
    first_name: '',
    last_name: '',
    email: '',
    Agencia: ''
  });
  // Estados para manejo de carga y mensajes
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [statusMessage, setStatusMessage] = useState<{text: string, type: 'success' | 'error'} | null>(null);

  // Cargar datos de usuarios desde la API
  useEffect(() => {
    const fetchUsers = async () => {
      setIsLoading(true);
      try {
        const usersData = await getAllUsers();
        setUsers(usersData);
        setStatusMessage(null);
      } catch (error) {
        console.error('Error al cargar usuarios:', error);
        setStatusMessage({
          text: 'Error al cargar la lista de usuarios',
          type: 'error'
        });
      } finally {
        setIsLoading(false);
      }
    };
    
    if (isOpen) {
      fetchUsers();
    }
  }, [isOpen]);

  // Manejar la edición de un usuario
  const handleEditUser = (user: UserData) => {
    setEditingUser({ ...user });
    setEditPassword(''); // Resetear el campo de contraseña al abrir el modal
    setShowEditModal(true);
  };

  // Manejar el guardado de un usuario editado
  const handleSaveEdit = async () => {
    if (!editingUser) return;

    setIsLoading(true);
    try {
      // Crear un objeto con los datos a actualizar
      const updateData: Partial<UserData> = { ...editingUser };
      
      // Solo incluir la contraseña si no está vacía
      if (editPassword.trim() !== '') {
        updateData.password = editPassword;
      }
      
      // Llamar a la API para actualizar el usuario
      await updateUser(editingUser.id, updateData);
      
      // Actualizar el estado local
      setUsers(prevUsers => 
        prevUsers.map(user => 
          user.id === editingUser.id ? {...editingUser} : user
        )
      );
      
      setShowEditModal(false);
      setEditingUser(null);
      setEditPassword(''); // Limpiar la contraseña al cerrar
      
      // Mostrar mensaje de éxito
      setStatusMessage({
        text: 'Usuario actualizado correctamente',
        type: 'success'
      });
      
      // Ocultar el mensaje después de 3 segundos
      setTimeout(() => setStatusMessage(null), 3000);
    } catch (error) {
      console.error('Error al actualizar usuario:', error);
      setStatusMessage({
        text: 'Error al actualizar el usuario',
        type: 'error'
      });
    } finally {
      setIsLoading(false);
    }
  };

  // Manejar la eliminación de un usuario
  const handleDeleteUser = async () => {
    if (!editingUser) return;

    setIsLoading(true);
    try {
      // Mostrar confirmación antes de eliminar
      const confirmDelete = window.confirm(`¿Estás seguro de eliminar al usuario ${editingUser.first_name} ${editingUser.last_name}?`);
      
      if (!confirmDelete) {
        setIsLoading(false);
        return;
      }
      
      // Llamar a la API para eliminar el usuario
      await deleteUser(editingUser.id);
      
      // Actualizar el estado local
      setUsers(prevUsers => prevUsers.filter(user => user.id !== editingUser.id));
      setShowEditModal(false);
      setEditingUser(null);
      setEditPassword(''); // Limpiar la contraseña
      
      // Mostrar mensaje de éxito
      setStatusMessage({
        text: 'Usuario eliminado correctamente',
        type: 'success'
      });
      
      // Ocultar el mensaje después de 3 segundos
      setTimeout(() => setStatusMessage(null), 3000);
    } catch (error) {
      console.error('Error al eliminar usuario:', error);
      setStatusMessage({
        text: 'Error al eliminar el usuario',
        type: 'error'
      });
    } finally {
      setIsLoading(false);
    }
  };

  // Manejar cambios en el formulario de edición
  const handleEditFormChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { name, value, type } = e.target;
    
    if (!editingUser) return;
    
    // Manejar el campo de contraseña separadamente
    if (name === 'password') {
      setEditPassword(value);
      return;
    }
    
    if (type === 'checkbox') {
      const checked = (e.target as HTMLInputElement).checked;
      setEditingUser(prev => {
        if (!prev) return prev;
        return { ...prev, [name]: checked };
      });
    } else {
      setEditingUser(prev => {
        if (!prev) return prev;
        return { ...prev, [name]: value };
      });
    }
  };

  // Manejar cambios en el formulario de nuevo usuario
  const handleNewUserChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { name, value, type } = e.target;
    
    if (type === 'checkbox') {
      const checked = (e.target as HTMLInputElement).checked;
      setNewUser(prev => ({ ...prev, [name]: checked }));
    } else {
      setNewUser(prev => ({ ...prev, [name]: value }));
    }
  };

  // Manejar la creación de un nuevo usuario
  const handleCreateUser = async () => {
    // Validar que los campos obligatorios no estén vacíos
    if (!newUser.email || !newUser.password || !newUser.first_name || !newUser.last_name) {
      alert('Todos los campos son obligatorios excepto la agencia');
      return;
    }
    
    setIsLoading(true);
    try {
      // Configurar la agencia (null si está vacía)
      const userData = {
        ...newUser,
        Agencia: newUser.Agencia === '' ? null : newUser.Agencia
      };
      
      // Llamar a la API para crear el usuario
      const createdUser = await createUser(userData as Omit<UserData, 'id'>);
      
      // Actualizar la lista de usuarios
      setUsers(prev => [...prev, createdUser]);
      
      // Limpiar el formulario
      setNewUser({
        password: '',
        is_superuser: false,
        first_name: '',
        last_name: '',
        email: '',
        Agencia: ''
      });
      
      // Mostrar mensaje de éxito
      setStatusMessage({
        text: 'Usuario creado correctamente',
        type: 'success'
      });
      
      // Ocultar el mensaje después de 3 segundos
      setTimeout(() => setStatusMessage(null), 3000);
    } catch (error) {
      console.error('Error al crear usuario:', error);
      setStatusMessage({
        text: 'Error al crear el usuario',
        type: 'error'
      });
    } finally {
      setIsLoading(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-lg w-4/5 max-w-6xl max-h-screen overflow-y-auto">
        <div className="p-6">
          <div className="flex justify-between items-center mb-6">
            <h2 className="text-2xl font-bold text-purple-800">Panel Administrativo de Usuarios</h2>
            <button 
              onClick={onClose}
              className="text-gray-500 hover:text-gray-700"
              disabled={isLoading}
            >
              <X size={24} />
            </button>
          </div>
          
          {/* Mensajes de estado */}
          {statusMessage && (
            <div className={`mb-4 p-3 rounded ${
              statusMessage.type === 'success' 
                ? 'bg-green-100 text-green-700 border border-green-300' 
                : 'bg-red-100 text-red-700 border border-red-300'
            }`}>
              {statusMessage.text}
            </div>
          )}
          
          {/* Indicador de carga */}
          {isLoading && (
            <div className="flex justify-center items-center py-4">
              <div className="animate-spin rounded-full h-8 w-8 border-t-2 border-b-2 border-purple-700"></div>
              <span className="ml-2 text-purple-700">Procesando...</span>
            </div>
          )}
          
          {/* Tabla de usuarios */}
          <div className="overflow-x-auto mb-8">
            <table className="min-w-full bg-gray-100 border border-gray-200 rounded-lg">
              <thead className="bg-purple-800 text-white">
                <tr>
                  <th className="px-4 py-3 text-left">ID</th>
                  <th className="px-4 py-3 text-left">Usuario</th>
                  <th className="px-4 py-3 text-left">Admin</th>
                  <th className="px-4 py-3 text-left">Nombre</th>
                  <th className="px-4 py-3 text-left">Email</th>
                  <th className="px-4 py-3 text-left">Agencia</th>
                  <th className="px-4 py-3 text-left">Acciones</th>
                </tr>
              </thead>
              <tbody>
                {users.map((user) => (
                  <tr key={user.id} className="border-t border-gray-200 hover:bg-gray-50">
                    <td className="px-4 py-3">{user.id}</td>
                    <td className="px-4 py-3">{user.first_name}</td>
                    <td className="px-4 py-3">
                      {user.is_superuser ? (
                        <span className="bg-green-100 text-green-800 py-1 px-2 rounded-full text-xs">Sí</span>
                      ) : (
                        <span className="bg-gray-100 text-gray-800 py-1 px-2 rounded-full text-xs">No</span>
                      )}
                    </td>
                    <td className="px-4 py-3">{`${user.first_name} ${user.last_name}`}</td>
                    <td className="px-4 py-3">{user.email}</td>
                    <td className="px-4 py-3">{user.Agencia || '-'}</td>
                    <td className="px-4 py-3">
                      <button 
                        onClick={() => handleEditUser(user)}
                        className="bg-purple-600 hover:bg-purple-700 text-white py-1 px-3 rounded-md text-sm disabled:opacity-50"
                        disabled={isLoading}
                      >
                        Editar
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          
          {/* Formulario para agregar nuevo usuario */}
          <div className="bg-gray-50 p-6 rounded-lg mb-6">
            <h3 className="text-xl font-semibold mb-4 text-purple-800">Agregar Nuevo Usuario</h3>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Nombre</label>
                <input
                  type="text"
                  name="first_name"
                  value={newUser.first_name}
                  onChange={handleNewUserChange}
                  className="w-full p-2 border border-gray-300 rounded-md bg-white"
                  placeholder="Nombre"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Apellido</label>
                <input
                  type="text"
                  name="last_name"
                  value={newUser.last_name}
                  onChange={handleNewUserChange}
                  className="w-full p-2 border border-gray-300 rounded-md bg-white"
                  placeholder="Apellido"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
                <input
                  type="email"
                  name="email"
                  value={newUser.email}
                  onChange={handleNewUserChange}
                  className="w-full p-2 border border-gray-300 rounded-md bg-white"
                  placeholder="Email"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Contraseña</label>
                <input
                  type="password"
                  name="password"
                  value={newUser.password}
                  onChange={handleNewUserChange}
                  className="w-full p-2 border border-gray-300 rounded-md bg-white"
                  placeholder="Contraseña"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Agencia</label>
                <input
                  type="text"
                  name="Agencia"
                  value={newUser.Agencia || ''}
                  onChange={handleNewUserChange}
                  className="w-full p-2 border border-gray-300 rounded-md bg-white"
                  placeholder="Agencia (opcional)"
                />
              </div>
              <div className="flex items-center mt-6">
                <input
                  type="checkbox"
                  name="is_superuser"
                  checked={newUser.is_superuser}
                  onChange={handleNewUserChange}
                  className="mr-2"
                  id="is_superuser_new"
                />
                <label htmlFor="is_superuser_new" className="text-sm font-medium text-gray-700">
                  Es Superusuario
                </label>
              </div>
            </div>
            <div className="mt-6">
              <button
                onClick={handleCreateUser}
                className="bg-green-600 hover:bg-green-700 text-white py-2 px-4 rounded-md disabled:opacity-50"
                disabled={isLoading}
              >
                {isLoading ? 'Procesando...' : 'Insertar Usuario'}
              </button>
            </div>
          </div>
        </div>
      </div>
      
      {/* Modal de edición */}
      {showEditModal && editingUser && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-lg w-full max-w-md p-6">
            <h3 className="text-xl font-semibold mb-4 text-purple-800">Editar Usuario</h3>
            
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">ID</label>
                <input
                  type="text"
                  value={editingUser.id}
                  disabled
                  className="w-full p-2 border border-gray-300 rounded-md bg-gray-100"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Nombre</label>
                <input
                  type="text"
                  name="first_name"
                  value={editingUser.first_name}
                  onChange={handleEditFormChange}
                  className="w-full p-2 border border-gray-300 rounded-md bg-white"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Apellido</label>
                <input
                  type="text"
                  name="last_name"
                  value={editingUser.last_name}
                  onChange={handleEditFormChange}
                  className="w-full p-2 border border-gray-300 rounded-md bg-white"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
                <input
                  type="email"
                  name="email"
                  value={editingUser.email}
                  onChange={handleEditFormChange}
                  className="w-full p-2 border border-gray-300 rounded-md bg-white"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Nueva Contraseña (dejar en blanco para mantener)</label>
                <input
                  type="password"
                  name="password"
                  value={editPassword}
                  onChange={handleEditFormChange}
                  className="w-full p-2 border border-gray-300 rounded-md bg-white"
                  placeholder="Nueva contraseña"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Agencia</label>
                <input
                  type="text"
                  name="Agencia"
                  value={editingUser.Agencia || ''}
                  onChange={handleEditFormChange}
                  className="w-full p-2 border border-gray-300 rounded-md bg-white"
                  placeholder="Agencia (opcional)"
                />
              </div>
              
              <div className="flex items-center">
                <input
                  type="checkbox"
                  name="is_superuser"
                  checked={editingUser.is_superuser}
                  onChange={handleEditFormChange}
                  className="mr-2"
                  id="is_superuser_edit"
                />
                <label htmlFor="is_superuser_edit" className="text-sm font-medium text-gray-700">
                  Es Superusuario
                </label>
              </div>
            </div>
            
            <div className="mt-6 flex justify-between">
              <button
                onClick={() => setShowEditModal(false)}
                className="bg-gray-300 hover:bg-gray-400 text-gray-800 py-2 px-4 rounded-md"
                disabled={isLoading}
              >
                Cancelar
              </button>
              
              <div className="flex space-x-2">
                <button
                  onClick={handleDeleteUser}
                  className="bg-red-600 hover:bg-red-700 text-white py-2 px-4 rounded-md disabled:opacity-50"
                  disabled={isLoading}
                >
                  {isLoading ? 'Procesando...' : 'Eliminar'}
                </button>
                <button
                  onClick={handleSaveEdit}
                  className="bg-purple-600 hover:bg-purple-700 text-white py-2 px-4 rounded-md disabled:opacity-50"
                  disabled={isLoading}
                >
                  {isLoading ? 'Procesando...' : 'Actualizar'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default AdminPanel;