import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Business from '../Business';
import Login from './Login';
import ProtectedRoute from './ProtectedRoute';
import SessionExpiryHandler from '../hooks/SessionExpiryHandler';

const AppRoutes: React.FC = () => {
  return (
    <BrowserRouter>
      <SessionExpiryHandler>
        <Routes>
          {/* Ruta principal, redirige a Business si está autenticado o a login si no */}
          <Route path="/" element={<Navigate to="/business" replace />} />
          
          {/* Ruta de login pública */}
          <Route path="/login" element={<Login />} />
          
          {/* Ruta protegida de Business */}
          <Route 
            path="/business" 
            element={
              <ProtectedRoute>
                <Business />
              </ProtectedRoute>
            } 
          />
          
          {/* Cualquier otra ruta redirige a Business */}
          <Route path="*" element={<Navigate to="/business" replace />} />
        </Routes>
      </SessionExpiryHandler>
    </BrowserRouter>
  );
};

export default AppRoutes;