import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import AgenciaSelector, { AgenciaNombre } from './AgenciaSelector';
import { logout, getCurrentUser } from '../service/AuthService';
import { IoLogOut } from "react-icons/io5";
import AdminPanel from './AdminPanel';
import Button from '@mui/material/Button';

interface NavbarProps {
  agenciaActual: AgenciaNombre;
  onAgenciaChange: (agencia: AgenciaNombre) => void;
  isLoading: boolean;
}

const Navbar: React.FC<NavbarProps> = ({ agenciaActual, onAgenciaChange, isLoading }) => {
  const navigate = useNavigate();
  const currentUser = getCurrentUser();
  const [isAdminPanelOpen, setIsAdminPanelOpen] = useState(false);

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  // Solo mostramos el botón de Admin si el usuario es superusuario
  const isSuperUser = currentUser?.isSuperuser;

  return (
    <>
      <div className="bg-[#493F91] shadow-md">
        <div className="w-full px-0">
          <div className="flex items-center h-14 relative">
            {/* Logo posicionado en el extremo izquierdo sin margen */}
            <div className="absolute left-5 flex items-center space-x-4">
              <img src="https://i.imgur.com/ghLRDuA.jpeg" alt="Logo" className="h-16 w-auto" />

              {/* Botón de Panel Admin - solo visible para superusuarios */}
              {isSuperUser && (
                <Button 
                  variant="contained" 
                  onClick={() => setIsAdminPanelOpen(true)} 
                  size="small"
                  sx={{ 
                    backgroundColor: '#1976d2', 
                    '&:hover': { backgroundColor: '#1565c0' },
                    textTransform: 'none',
                    fontSize: '0.875rem',
                    padding: '0.25rem 1rem'
                  }}
                >
                  Panel Admin
                </Button>
              )}
            </div>

            {/* Selector de Agencias centrado */}
            <div className="mx-auto w-40">
              <AgenciaSelector
                agenciaActual={agenciaActual}
                onAgenciaChange={onAgenciaChange}
                cargando={isLoading}
              />
            </div>

            {/* Información de usuario y botón de cierre de sesión */}
            <div className="absolute right-5 flex items-center space-x-4">
              {currentUser && (
                <div className="text-white text-xs">
                  <span className="font-medium">{currentUser.firstName} {currentUser.lastName}</span>
                </div>
              )}
              <Button
                variant="contained"
                onClick={handleLogout}
                size="small"
                startIcon={<IoLogOut />}
                sx={{ 
                  backgroundColor: '#dc2626', 
                  '&:hover': { backgroundColor: '#b91c1c' },
                  textTransform: 'none',
                  fontSize: '0.875rem',
                  padding: '0.25rem 0.75rem'
                }}
              >
                Cerrar sesión
              </Button>
            </div>
          </div>
        </div>
      </div>

      {/* Panel de Administración como componente separado */}
      <AdminPanel
        isOpen={isAdminPanelOpen}
        onClose={() => setIsAdminPanelOpen(false)}
      />
    </>
  );
};

export default Navbar;