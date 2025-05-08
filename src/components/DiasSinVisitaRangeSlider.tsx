import { debounce } from 'lodash';
import React, { useState, useEffect, useCallback } from 'react';

// Definimos el tipo para las props
interface DiasSinVisitaRangeSliderProps {
  onRangeChange: (minValue: number, maxValue: number) => void;
  initialMin?: number;
  initialMax?: number;
  absoluteMin?: number;
  absoluteMax?: number; // Ahora este valor será calculado dinámicamente
}

const DiasSinVisitaRangeSlider: React.FC<DiasSinVisitaRangeSliderProps> = ({
  onRangeChange,
  initialMin = 0,
  initialMax = 4800,
  absoluteMin = 0,
  absoluteMax = 4800 // El valor por defecto, pero será reemplazado por el calculado
}) => {
  const [minValue, setMinValue] = useState<number>(initialMin);
  const [maxValue, setMaxValue] = useState<number>(initialMax);
  const [minInputValue, setMinInputValue] = useState<string>(initialMin.toString());
  const [maxInputValue, setMaxInputValue] = useState<string>(initialMax.toString());

  // Estado para mostrar un tooltip con información sobre el rango máximo
  const [showMaxInfo, setShowMaxInfo] = useState<boolean>(false);

  // Aplicar debounce para evitar actualizaciones excesivas
  const debouncedOnRangeChange = useCallback(
    debounce((min: number, max: number) => {
      onRangeChange(min, max);
    }, 300),
    [onRangeChange]
  );

  // Handler para el input de valor mínimo
  const handleMinInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const inputValue = e.target.value;
    setMinInputValue(inputValue);
  };

  // Handler para el input de valor máximo
  const handleMaxInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const inputValue = e.target.value;
    setMaxInputValue(inputValue);
  };

  // Función para validar y aplicar el valor mínimo cuando el usuario termina de editar
  const handleMinInputBlur = () => {
    let value = parseInt(minInputValue, 10);

    // Validaciones
    if (isNaN(value)) {
      value = minValue;
    } else if (value < absoluteMin) {
      value = absoluteMin;
    } else if (value > maxValue) {
      value = maxValue;
    }

    setMinValue(value);
    setMinInputValue(value.toString());

    // Notificar al componente padre sobre el cambio
    debouncedOnRangeChange(value, maxValue);
  };

  // Función para validar y aplicar el valor máximo cuando el usuario termina de editar
  const handleMaxInputBlur = () => {
    let value = parseInt(maxInputValue, 10);

    // Validaciones
    if (isNaN(value)) {
      value = maxValue;
    } else if (value > absoluteMax) {
      // Si el valor ingresado es mayor que el máximo calculado, usar el máximo
      value = absoluteMax;
    } else if (value < minValue) {
      value = minValue;
    }

    setMaxValue(value);
    setMaxInputValue(value.toString());

    // Notificar al componente padre sobre el cambio
    debouncedOnRangeChange(minValue, value);
  };

  // Manejador para la tecla Enter en los inputs
  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>, isMin: boolean) => {
    if (e.key === 'Enter') {
      if (isMin) {
        handleMinInputBlur();
      } else {
        handleMaxInputBlur();
      }
      // Quitar el foco del input
      e.currentTarget.blur();
    }
  };

  // Actualizar valores iniciales y límites si cambian las props
  // Modificación recomendada para el useEffect
  useEffect(() => {
    // Actualizar solo cuando las props cambian, no los valores de estado internos
    const shouldUpdateMin = initialMin !== minValue;
    const shouldUpdateMax = initialMax !== maxValue;
    const shouldCapMax = absoluteMax < maxValue;

    // Actualizar solo si es necesario, y solo una vez por cambio de props
    if (shouldUpdateMin) {
      setMinValue(initialMin);
      setMinInputValue(initialMin.toString());
    }

    if (shouldUpdateMax) {
      setMaxValue(initialMax);
      setMaxInputValue(initialMax.toString());
    }

    // Manejar el caso donde el valor máximo debe ser limitado
    if (shouldCapMax) {
      setMaxValue(absoluteMax);
      setMaxInputValue(absoluteMax.toString());

      // Solo llamar a la función de cambio si realmente estamos cambiando el valor
      if (absoluteMax !== maxValue) {
        debouncedOnRangeChange(minValue, absoluteMax);
      }
    }

    // No incluir minValue ni maxValue en la lista de dependencias
  }, [initialMin, initialMax, absoluteMax, debouncedOnRangeChange]);

  // Limpiar el debounce cuando el componente se desmonte
  useEffect(() => {
    return () => {
      debouncedOnRangeChange.cancel();
    };
  }, [debouncedOnRangeChange]);

  return (
    <div className="flex items-center space-x-2 relative">
      <span
        className="text-sm text-gray-600 whitespace-nowrap"
        onMouseEnter={() => setShowMaxInfo(true)}
        onMouseLeave={() => setShowMaxInfo(false)}
      >
        Días sin venir a taller
      </span>

      {/* Tooltip informativo */}
      {showMaxInfo && (
        <div className="absolute -top-10 left-0 bg-gray-800 text-white text-xs rounded py-1 px-2 z-10">
          Rango actual: {absoluteMin} a {absoluteMax} días
        </div>
      )}

      <div className="flex items-center space-x-1">
        <input
          type="text"
          value={minInputValue}
          onChange={handleMinInputChange}
          onBlur={handleMinInputBlur}
          onKeyDown={(e) => handleKeyDown(e, true)}
          className="w-12 px-2 py-1 text-sm text-blue-600 font-medium rounded-md border border-gray-200 text-center"
          aria-label="Valor mínimo"
          title={`Valor mínimo (0-${absoluteMax})`}
        />
        <span className="text-sm text-gray-500">a</span>
        <input
          type="text"
          value={maxInputValue}
          onChange={handleMaxInputChange}
          onBlur={handleMaxInputBlur}
          onKeyDown={(e) => handleKeyDown(e, false)}
          className="w-12 px-2 py-1 text-sm text-blue-600 font-medium rounded-md border border-gray-200 text-center"
          aria-label="Valor máximo"
          title={`Valor máximo (0-${absoluteMax})`}
        />
      </div>
    </div>
  );
};

export default DiasSinVisitaRangeSlider;