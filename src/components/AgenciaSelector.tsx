import React, { useState, useEffect, useRef } from 'react';
import { ChevronDown } from 'lucide-react';
import { getAccessibleAgencias, getCurrentUser } from '../service/AuthService';

// Definir las agencias disponibles como un tipo para mayor seguridad
export type AgenciaNombre = 'Gran Auto' | 'Gasme' | 'Sierra' | 'Huerpel' | 'Del Bravo';

export interface AgenciaInfo {
  nombre: AgenciaNombre;
  archivo: string;
  color: string;
  activa: boolean;
}

interface AgenciaSelectorProps {
  agenciaActual: AgenciaNombre;
  onAgenciaChange: (agencia: AgenciaNombre) => void;
  cargando: boolean;
}

const AgenciaSelector: React.FC<AgenciaSelectorProps> = ({ 
  agenciaActual, 
  onAgenciaChange,
  cargando
}) => {
  const [menuAbierto, setMenuAbierto] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const currentUser = getCurrentUser();
  const accessibleAgencias = getAccessibleAgencias();

  // Lista completa de agencias disponibles
  const todasLasAgencias: AgenciaInfo[] = [
    { nombre: 'Gran Auto', archivo: 'granauto.csv', color: '#493F91', activa: true },
    { nombre: 'Gasme', archivo: 'gasme.csv', color: '#493F91', activa: true },
    { nombre: 'Sierra', archivo: 'sierra.csv', color: '#493F91', activa: true },
    { nombre: 'Huerpel', archivo: 'huerpel.csv', color: '#493F91', activa: true },
    { nombre: 'Del Bravo', archivo: 'delbravo.csv', color: '#493F91', activa: true },
  ];

  // Filtrar solo las agencias a las que el usuario tiene acceso
  const agencias = todasLasAgencias.filter(agencia => 
    accessibleAgencias.includes(agencia.nombre)
  );

  // Si el usuario solo tiene acceso a una agencia, no necesitamos mostrar el selector
  const mostrarSelector = agencias.length > 1;

  // Cerrar el menú cuando se hace clic fuera de él
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setMenuAbierto(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);

  // Manejar la selección de una agencia
  const handleAgenciaSelect = (agencia: AgenciaNombre) => {
    if (cargando) return; // No permitir cambios durante la carga
    onAgenciaChange(agencia);
    setMenuAbierto(false);
  };

  return (
    <div className="relative" ref={menuRef}>
      {/* Si el usuario solo tiene acceso a una agencia, mostrar solo el nombre */}
      {!mostrarSelector ? (
        <div className="flex items-center justify-center bg-[#493F91] text-white px-4 py-2 rounded-none w-full">
          <span>{agenciaActual}</span>
        </div>
      ) : (
        <>
          {/* Botón para abrir/cerrar el menú */}
          <button
            className={`flex items-center justify-between bg-[#493F91] hover:bg-[#493F91] text-white px-4 py-2 rounded-none w-full transition-colors ${cargando ? 'opacity-70 cursor-not-allowed' : ''}`}
            onClick={() => !cargando && setMenuAbierto(!menuAbierto)}
            disabled={cargando}
          >
            <span>Agencias</span>
            <ChevronDown className="w-4 h-4 ml-1" />
          </button>

          {/* Menú desplegable */}
          {menuAbierto && (
            <div className="absolute w-full z-50 mt-1 bg-white border border-gray-200 shadow-lg rounded-sm">
              {agencias.map((agencia) => (
                <button
                  key={agencia.nombre}
                  className={`w-full text-left px-4 py-2 hover:bg-gray-100 ${agenciaActual === agencia.nombre ? 'bg-gray-50 font-medium text-[#493F91]' : ''}`}
                  onClick={() => handleAgenciaSelect(agencia.nombre)}
                  disabled={!agencia.activa || cargando}
                >
                  {agencia.nombre}
                  {agenciaActual === agencia.nombre && (
                    <span className="ml-2 text-xs bg-[#493F91] text-white px-1.5 py-0.5 rounded">
                      Activa
                    </span>
                  )}
                </button>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default AgenciaSelector;