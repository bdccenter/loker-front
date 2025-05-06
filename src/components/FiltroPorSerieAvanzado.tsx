import React, { useState, useEffect, useRef } from 'react';
import { Search, X, Tag } from 'lucide-react';

interface FiltroPorSerieAvanzadoProps {
  value: string;
  onChange: (value: string) => void;
  onSearch: () => void;
  placeholder?: string;
  className?: string;
  historialBusquedas?: string[];
  onAddToHistory?: (term: string) => void;
}

const FiltroPorSerieAvanzado: React.FC<FiltroPorSerieAvanzadoProps> = ({
  value,
  onChange,
  onSearch,
  placeholder = "Buscar por serie",
  className = "",
  historialBusquedas = [],
  onAddToHistory
}) => {
  const [inputValue, setInputValue] = useState<string>(value);
  const [showClearButton, setShowClearButton] = useState<boolean>(false);
  const [showDropdown, setShowDropdown] = useState<boolean>(false);
  const [multipleMode, setMultipleMode] = useState<boolean>(false);
  const [selectedTerms, setSelectedTerms] = useState<string[]>([]);
  
  const inputRef = useRef<HTMLInputElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  
  // Manejar clics fuera del dropdown para cerrarlo
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setShowDropdown(false);
      }
    };
    
    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);
  
  // Actualizar el estado local cuando cambia el valor externo
  useEffect(() => {
    // Solo actualizar si no estamos en modo múltiple
    if (!multipleMode) {
      setInputValue(value);
      setShowClearButton(value.length > 0);
    }
  }, [value, multipleMode]);
  
  // Manejar cambios en el input
  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = e.target.value;
    setInputValue(newValue);
    setShowClearButton(newValue.length > 0);
    
    if (!multipleMode) {
      onChange(newValue);
    }
  };
  
  // Limpiar el campo de búsqueda
  const handleClear = () => {
    setInputValue('');
    setShowClearButton(false);
    if (multipleMode) {
      setSelectedTerms([]);
      onChange('');
    } else {
      onChange('');
    }
  };
  
  // Manejar la tecla Enter para búsqueda
  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      if (multipleMode) {
        if (inputValue.trim()) {
          addTerm(inputValue.trim());
          setInputValue('');
        }
      } else {
        if (inputValue.trim() && onAddToHistory) {
          onAddToHistory(inputValue.trim());
        }
        onSearch();
      }
    }
  };
  
  // Agregar un término a la búsqueda múltiple
  const addTerm = (term: string) => {
    if (!selectedTerms.includes(term)) {
      const newTerms = [...selectedTerms, term];
      setSelectedTerms(newTerms);
      onChange(newTerms.join(','));
      
      if (onAddToHistory) {
        onAddToHistory(term);
      }
    }
  };
  
  // Remover un término de la búsqueda múltiple
  const removeTerm = (term: string) => {
    const newTerms = selectedTerms.filter(t => t !== term);
    setSelectedTerms(newTerms);
    onChange(newTerms.join(','));
  };
  
  // Cambiar entre modo simple y múltiple
  const toggleMultipleMode = () => {
    const newMode = !multipleMode;
    setMultipleMode(newMode);
    
    if (newMode) {
      // Al activar el modo múltiple, convertir el término actual en un tag si existe
      if (value) {
        setSelectedTerms([value]);
      } else {
        setSelectedTerms([]);
      }
      setInputValue('');
    } else {
      // Al desactivar el modo múltiple, convertir los términos en una búsqueda simple
      onChange(selectedTerms.join(' '));
      setInputValue(selectedTerms.join(' '));
      setSelectedTerms([]);
    }
  };
  
  return (
    <div className={`relative ${className}`} ref={dropdownRef}>
      <div className="relative flex items-center">
        <Search className="absolute left-2 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
        <input
          ref={inputRef}
          type="text"
          value={inputValue}
          onChange={handleInputChange}
          onKeyDown={handleKeyDown}
          placeholder={multipleMode ? "Añadir series..." : placeholder}
          className="border border-gray-300 rounded-md pl-8 pr-16 py-2 text-sm w-full focus:border-red-600 focus:ring-1 focus:ring-red-600 focus:outline-none"
          autoComplete="off"
        />
        
        {/* Botón para alternar modo */}
        <button
          type="button"
          onClick={toggleMultipleMode}
          className="absolute right-8 top-1/2 transform -translate-y-1/2 text-gray-500 hover:text-gray-700"
          title={multipleMode ? "Modo búsqueda simple" : "Modo búsqueda múltiple"}
        >
          <Tag className="w-4 h-4" />
        </button>
        
        {/* Botón para limpiar */}
        {showClearButton && (
          <button
            type="button"
            onClick={handleClear}
            className="absolute right-2 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-gray-600"
            aria-label="Limpiar búsqueda"
          >
            <X className="w-4 h-4" />
          </button>
        )}
      </div>
      
      {/* Área para mostrar tags en modo múltiple */}
      {multipleMode && selectedTerms.length > 0 && (
        <div className="flex flex-wrap gap-1 mt-2">
          {selectedTerms.map((term, index) => (
            <div key={index} className="flex items-center bg-red-100 text-red-800 text-xs px-2 py-1 rounded">
              {term}
              <button 
                onClick={() => removeTerm(term)}
                className="ml-1 text-red-600 hover:text-red-800"
              >
                <X className="w-3 h-3" />
              </button>
            </div>
          ))}
        </div>
      )}
      
      {/* Dropdown para historial de búsquedas */}
      {showDropdown && historialBusquedas.length > 0 && (
        <div className="absolute z-10 mt-1 w-full bg-white shadow-lg rounded-md border border-gray-200 overflow-hidden">
          <div className="max-h-64 overflow-y-auto">
            {historialBusquedas.map((term, index) => (
              <div 
                key={index}
                className="px-4 py-2 text-sm hover:bg-gray-100 cursor-pointer flex items-center"
                onClick={() => {
                  if (multipleMode) {
                    addTerm(term);
                    setInputValue('');
                  } else {
                    setInputValue(term);
                    onChange(term);
                    setShowDropdown(false);
                  }
                }}
              >
                <Search className="w-3 h-3 text-gray-500 mr-2" />
                {term}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default FiltroPorSerieAvanzado;