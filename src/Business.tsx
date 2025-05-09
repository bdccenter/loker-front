import React, { useEffect, useState, useMemo, useCallback } from 'react';
import { Search, ChevronDown, Calendar } from 'lucide-react';
import DiasSinVisitaRangeSlider from './components/DiasSinVisitaRangeSlider';
import { obtenerClientesPaginados, Cliente, limpiarCacheCSV, obtenerMetadatosFiltros, establecerAgenciaActual, configuracionAgencias } from './CsvDataService';
import FiltroPorSerieAvanzado from './components/FiltroPorSerieAvanzado';
import { obtenerHistorialBusquedas, guardarEnHistorial } from './service/HistorialBusquedas';
import { debounce } from 'lodash';
import FilterLoader from './components/FilterLoader';
import ExportCSVButton from './components/ExportCSVButton';
import Navbar from './components/Navbar';
import { AgenciaNombre } from './components/AgenciaSelector';
import { getCurrentUser, getAccessibleAgencias } from './service/AuthService';
import { getAgencyLogoUrl } from './utilis/AgencyLogoHelper';
import Button from '@mui/material/Button';
import { FormControl, InputLabel, Select, MenuItem, Checkbox, ListItemText, OutlinedInput } from '@mui/material';



type AgenciasType = {
  [key: string]: boolean;
};

type PaquetesType = {
  [key: string]: boolean;
};

type APSType = {
  [key: string]: boolean;
};

const formatearNumeroTelefonoParaMostrar = (numero?: string): string => {
  if (!numero || numero.trim() === '') {
    return '-';
  }

  // Si el número está en notación científica, convertirlo a formato normal
  if (numero.includes('E') || numero.includes('e')) {
    try {
      // Convertir de notación científica a número normal sin decimales
      const num = Number(numero);
      if (isNaN(num)) {
        return numero; // Si no se puede convertir, devolver el original
      }
      return num.toFixed(0); // Convertir a string sin decimales
    } catch (e) {
      console.error('Error al convertir número de notación científica:', e);
      return numero; // Devolver el original si hay error
    }
  }

  return numero;
};

// Función para verificar si un campo está vacío o solo contiene espacios
const isEmpty = (value?: string): boolean => {
  return !value || value.trim() === '';
};
// Función  para determinar el número de Cloudtalk
const determinaCloudTalk = (cliente: Cliente): string => {
  // Verificar celular primero
  if (!isEmpty(cliente.celular)) {
    const celularFormateado = formatearNumeroTelefonoParaMostrar(cliente.celular);
    return celularFormateado !== '-' ? `+${celularFormateado}` : '-';
  }

  // Si no hay celular, verificar teléfono
  if (!isEmpty(cliente.telefono)) {
    const telefonoFormateado = formatearNumeroTelefonoParaMostrar(cliente.telefono);
    return telefonoFormateado !== '-' ? `+${telefonoFormateado}` : '-';
  }

  // Si no hay teléfono, verificar T. oficina
  if (!isEmpty(cliente.tOficina)) {
    const oficinaFormateado = formatearNumeroTelefonoParaMostrar(cliente.tOficina);
    return oficinaFormateado !== '-' ? `+${oficinaFormateado}` : '-';
  }

  // Si no hay ninguno, mostrar guión
  return '-';
};

const formatearFechaTabla = (fecha: Date): string => {
  if (!fecha || isNaN(fecha.getTime())) return "-";

  const dia = fecha.getDate();
  const mes = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'][fecha.getMonth()];
  const año = fecha.getFullYear();

  return `${dia} ${mes} ${año}`;
};



function App() {
  // Estados para la carga de datos
  const [isFiltering, setIsFiltering] = useState<boolean>(false);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [errorCarga, setErrorCarga] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [mostrarFiltroAgencia, setMostrarFiltroAgencia] = useState<boolean>(false);
  const [mostrarFiltroModelo, setMostrarFiltroModelo] = useState<boolean>(false);
  const [mostrarFiltroAño, setMostrarFiltroAño] = useState<boolean>(false);
  const [posicionMenu, setPosicionMenu] = useState({ top: 0, left: 0, width: 0 });
  const [mostrarFiltroPaquete, setMostrarFiltroPaquete] = useState<boolean>(false);
  const [mostrarFiltroAPS, setMostrarFiltroAPS] = useState<boolean>(false);
  const [paquetesDisponibles, setPaquetesDisponibles] = useState<string[]>([]);
  const [agenciasDisponibles, setAgenciasDisponibles] = useState<string[]>([]);
  const [modelosDisponibles, setModelosDisponibles] = useState<string[]>([]);
  const [añosDisponibles, setAñosDisponibles] = useState<string[]>([]);
  const [asesoresDisponibles, setAsesoresDisponibles] = useState<string[]>([]);
  const [agenciasSeleccionadas, setAgenciasSeleccionadas] = useState<AgenciasType>(() => ({}));
  const [historialBusquedas, setHistorialBusquedas] = useState<string[]>([]);

  

  // estados de HUD de Agencias
  // Estado para la agencia seleccionada (NUEVO)
  const [agenciaActual, setAgenciaActual] = useState<AgenciaNombre>(() => {
    const currentUser = getCurrentUser();
    const accessibleAgencias = getAccessibleAgencias();

    console.log('Current user:', currentUser);
    console.log('Accessible agencias:', accessibleAgencias);

    // Comprobar si el usuario y las agencias existen
    if (!currentUser || !accessibleAgencias || accessibleAgencias.length === 0) {
      return 'Gran Auto'; // Valor predeterminado seguro
    }

    // El resto de tu lógica...
    if (currentUser?.agencia && accessibleAgencias.includes(currentUser.agencia as AgenciaNombre)) {
      return currentUser.agencia as AgenciaNombre;
    }

    if (accessibleAgencias.length > 0) {
      return accessibleAgencias[0] as AgenciaNombre;
    }

    return 'Gran Auto';
  });

  const [cambiandoAgencia, setCambiandoAgencia] = useState<boolean>(false);
  // estados de boton siguiente y anterior
  const [currentPage, setCurrentPage] = useState<number>(1);
  const itemsPerPage = 700;

  const [cargandoPagina, setCargandoPagina] = useState<boolean>(false);
  const [totalRegistros, setTotalRegistros] = useState<number>(0);
  // Por estas correctas:
  const [minDiasSinVisita, setMinDiasSinVisita] = useState<number>(0);
  const [maxDiasSinVisita, setMaxDiasSinVisita] = useState<number>(10000); // Aumentado para incluir todos los registros
  const [maxDiasSinVenirCalculado, setMaxDiasSinVenirCalculado] = useState<number>(10000); // También aumentado


  // Agregar estos estados junto a los demás estados al inicio de la función App()
  const [filtroNombreFactura, setFiltroNombreFactura] = useState<string>('');
  const [filtroCelular, setFiltroCelular] = useState<string>('');
  // estados de la base de datos .CSV 
  const [clientesData, setClientesData] = useState<Cliente[]>([]);
  // Estados para el calendario
  const [mostrarCalendario, setMostrarCalendario] = useState<boolean>(false);
  const [fechaInicio, setFechaInicio] = useState<Date | null>(null);
  const [fechaFin, setFechaFin] = useState<Date | null>(null);
  const [mesInicio, setMesInicio] = useState<Date>(new Date());
  const [mesFin, setMesFin] = useState<Date>(new Date(new Date().getFullYear(), new Date().getMonth() + 2, 1));
  const [seleccionandoInicio, setSeleccionandoInicio] = useState<boolean>(true);
  const [mostrarSelectorMesInicio, setMostrarSelectorMesInicio] = useState<boolean>(false);
  const [mostrarSelectorMesFin, setMostrarSelectorMesFin] = useState<boolean>(false);
  const [selectorAñoInicio, setSelectorAñoInicio] = useState<number>(new Date().getFullYear());
  const [selectorAñoFin, setSelectorAñoFin] = useState<number>(new Date().getFullYear());
  const [todosLosDatosCargados, setTodosLosDatosCargados] = useState<boolean>(false);




  // Manejador para cambiar de agencia
  const handleAgenciaChange = useCallback((nuevaAgencia: AgenciaNombre) => {
    if (nuevaAgencia === agenciaActual) return;

    console.log(`Cambiando de agencia: ${agenciaActual} -> ${nuevaAgencia}`);

    setCambiandoAgencia(true);
    setIsLoading(true);
    setErrorCarga(null);
    setTodosLosDatosCargados(false);

    // Limpiar todos los datos y filtros actuales
    setClientesData([]);
    setCurrentPage(1);

    // Actualizar el estado de la agencia
    setAgenciaActual(nuevaAgencia);

    // Establecer la nueva agencia en el servicio de datos
    establecerAgenciaActual(nuevaAgencia);

    // La carga de datos se realizará en el efecto useEffect que observa todosLosDatosCargados
  }, [agenciaActual]);
  // Función para cargar datos paginados
  const cargarDatosPaginados = useCallback(async (pagina: number) => {
    setCargandoPagina(true);
    try {
      console.log(`Cargando todos los datos del CSV...`);
      // Se cambia el parámetro precargaCompleta a true para cargar todos los datos
      const resultado = await obtenerClientesPaginados(pagina, itemsPerPage, true);

      // MODIFICACIÓN: Ordenar los datos por fecha de última visita de manera descendente (más reciente primero)
      const datosOrdenados = [...resultado.clientes].sort((a, b) => {
        // Si ambos tienen fecha de última visita, comparar fechas
        if (a.ultimaVisita && b.ultimaVisita) {
          return b.ultimaVisita.getTime() - a.ultimaVisita.getTime();
        }
        // Si solo a tiene fecha, a va primero
        else if (a.ultimaVisita) {
          return -1;
        }
        // Si solo b tiene fecha, b va primero
        else if (b.ultimaVisita) {
          return 1;
        }
        // Si ninguno tiene fecha, mantener el orden original
        return 0;
      });

      // Guardamos todos los datos ordenados en el estado
      setClientesData(datosOrdenados);
      setTotalRegistros(resultado.total);
      setTodosLosDatosCargados(true);

      // Actualizar filtros con todos los datos disponibles
      if (datosOrdenados.length > 0) {
        // Extraer agencias únicas
        const agencias = Array.from(new Set(datosOrdenados.map(cliente => cliente.agencia)))
          .filter(agencia => agencia)
          .sort();
        setAgenciasDisponibles(agencias);

        // Inicializar los checkboxes de agencias
        const agenciasObj: AgenciasType = {};
        agencias.forEach(agencia => {
          if (agencia) {
            agenciasObj[agencia] = true;
          }
        });
        setAgenciasSeleccionadas(agenciasObj);

        // Extraer paquetes únicos
        const paquetes = Array.from(new Set(datosOrdenados.map(cliente => cliente.paquete || 'null')))
          .filter(paquete => paquete)
          .sort();
        setPaquetesDisponibles(paquetes);

        // Inicializar checkboxes de paquetes
        const paquetesObj: PaquetesType = {};
        paquetes.forEach(paquete => {
          if (paquete) {
            paquetesObj[paquete] = true;
          }
        });
        setPaquetesSeleccionados(paquetesObj);

        // Extraer modelos únicos
        const modelos = Array.from(new Set(datosOrdenados.map(cliente => cliente.modelo)))
          .filter(modelo => modelo)
          .sort();
        setModelosDisponibles(modelos);

        // Extraer años únicos
        const años = Array.from(new Set(datosOrdenados.map(cliente => cliente.año.toString())))
          .filter(año => año)
          .sort((a, b) => Number(b) - Number(a));
        setAñosDisponibles(años);

        // Extraer asesores APS únicos
        const asesores = Array.from(new Set(datosOrdenados.map(cliente => cliente.aps)))
          .filter((asesor): asesor is string => asesor !== undefined && asesor !== null)
          .sort();
        setAsesoresDisponibles(asesores);
      }

      console.log(`Cargados ${datosOrdenados.length} registros de ${resultado.total} totales`);
      return true;
    } catch (error) {
      console.error('Error al cargar los datos:', error);
      setErrorCarga(error instanceof Error ? error.message : 'Error desconocido al cargar datos');
      return false;
    } finally {
      setCargandoPagina(false);
      setIsLoading(false);
      setCambiandoAgencia(false);
    }
  }, [itemsPerPage, agenciaActual]); // Agregamos agenciaActual como dependencia



  // Efecto para cargar datos iniciales
  useEffect(() => {
    const inicializarDatos = async () => {
      setIsLoading(true);
      setErrorCarga(null);
      try {
        // Primero cargar los metadatos para los filtros
        console.log('Cargando metadatos de filtros...');
        const metadatos = await obtenerMetadatosFiltros(agenciaActual);

        console.log('Metadatos obtenidos:', {
          agencias: metadatos.agencias.length,
          modelos: metadatos.modelos.length,
          años: metadatos.años.length,
          paquetes: metadatos.paquetes.length,
          asesores: metadatos.asesores.length
        });

        // Actualizar los estados con los metadatos completos
        setAgenciasDisponibles(metadatos.agencias);
        setModelosDisponibles(metadatos.modelos);
        setAñosDisponibles(metadatos.años);
        setAsesoresDisponibles(metadatos.asesores);

        // Inicializar los checkboxes con todos los valores activados
        const agenciasObj: AgenciasType = {};
        metadatos.agencias.forEach(agencia => {
          if (agencia) { // Asegurarse de que no es undefined o ""
            agenciasObj[agencia] = true;
          }
        });
        setAgenciasSeleccionadas(agenciasObj);

        // Actualizar paquetes disponibles
        setPaquetesDisponibles(metadatos.paquetes);

        // Inicializar checkboxes de paquetes
        const paquetesObj: PaquetesType = {};
        metadatos.paquetes.forEach(paquete => {
          if (paquete) {
            paquetesObj[paquete] = true;
          }
        });
        setPaquetesSeleccionados(paquetesObj);

        // Inicializar checkboxes de modelos
        const modelosObj: { [key: string]: boolean } = {};
        metadatos.modelos.forEach(modelo => {
          modelosObj[modelo] = true;
        });
        setModelosSeleccionados(modelosObj);

        // Inicializar checkboxes de años
        const añosObj: { [key: string]: boolean } = {};
        metadatos.años.forEach(año => {
          añosObj[año] = true;
        });
        setAñosSeleccionados(añosObj);

        // Inicializar checkboxes de asesores APS
        const apsObj: APSType = {};
        metadatos.asesores.forEach(asesor => {
          apsObj[asesor] = true;
        });
        setAPSSeleccionados(apsObj);

        // Ahora cargar todos los datos
        await cargarDatosPaginados(1);

      } catch (error) {
        console.error('Error al inicializar datos:', error);
        setErrorCarga(error instanceof Error ? error.message : 'Error desconocido al cargar datos');
      } finally {
        setIsLoading(false);
      }
    };

    if (!todosLosDatosCargados) {
      inicializarDatos();
    }
  }, [cargarDatosPaginados, todosLosDatosCargados, agenciaActual, cambiandoAgencia]);


  useEffect(() => {
    if (agenciasDisponibles.length > 0) {
      console.log("agenciasDisponibles cambió:", agenciasDisponibles.length);

      // Forzar actualización de agenciasSeleccionadas cuando cambia agenciasDisponibles
      const agenciasObj: AgenciasType = {};
      agenciasDisponibles.forEach(agencia => {
        if (agencia) {
          agenciasObj[agencia] = true;
        }
      });

      // Actualizar el estado solo si hay nuevas agencias o si la cantidad es diferente
      if (Object.keys(agenciasObj).length !== Object.keys(agenciasSeleccionadas).length) {
        console.log("Actualizando agenciasSeleccionadas con nuevos valores:", Object.keys(agenciasObj).length);
        setAgenciasSeleccionadas(agenciasObj);
      } else {
        // Verificar si alguna agencia en agenciasDisponibles no existe en agenciasSeleccionadas
        const nuevaAgenciaDetectada = agenciasDisponibles.some(agencia => !agenciasSeleccionadas.hasOwnProperty(agencia));
        if (nuevaAgenciaDetectada) {
          console.log("Detectada nueva agencia en agenciasDisponibles, actualizando agenciasSeleccionadas");
          setAgenciasSeleccionadas(agenciasObj);
        }
      }
    }
  }, [agenciasDisponibles, agenciasSeleccionadas]); // Dependencias actualizadas



  useEffect(() => {
    setHistorialBusquedas(obtenerHistorialBusquedas());
  }, []);

  const addToSearchHistory = (term: string) => {
    const nuevoHistorial = guardarEnHistorial(term);
    setHistorialBusquedas(nuevoHistorial);
  };

  const debouncedSetSearchTerm = useMemo(
    () => debounce((value: string) => {
      setSearchTerm(value);
    }, 300), // 300ms de retraso
    []
  );

  useEffect(() => {
    return () => {
      debouncedSetSearchTerm.cancel();
    };
  }, [debouncedSetSearchTerm]);

  // Función para realizar búsqueda inmediata (sin debounce)
  const handleSearch = () => {
    if (searchTerm.trim()) {
      addToSearchHistory(searchTerm.trim());
    }
  };

  // Inicializar el estado de los asesores APS seleccionados - ahora se hace dinámicamente
  const [apsSeleccionados, setAPSSeleccionados] = useState<APSType>({});


  useEffect(() => {
    if (clientesData.length > 0) {
      // Obtener agencias únicas del CSV
      const agencias = Array.from(new Set(clientesData.map(cliente => cliente.agencia)))
        .filter(agencia => agencia)
        .sort();
      setAgenciasDisponibles(agencias);

      // Inicializar los checkboxes de agencias
      const agenciasObj: AgenciasType = {};
      agencias.forEach(agencia => {
        agenciasObj[agencia] = true;
      });
      setAgenciasSeleccionadas(agenciasObj);

      // Obtener paquetes únicos del CSV
      const paquetes = Array.from(new Set(clientesData.map(cliente =>
        cliente.paquete !== undefined ? cliente.paquete : 'null'
      )))
        .filter(paquete => paquete !== undefined)
        .sort();

      // Verificamos si hay paquetes duplicados debido a conversión numérica
      console.log('Paquetes únicos encontrados:', paquetes);

      setPaquetesDisponibles(paquetes);

      // Inicializar los checkboxes de paquetes
      const paquetesObj: PaquetesType = {};
      paquetes.forEach(paquete => {
        paquetesObj[paquete] = true;
      });
      setPaquetesSeleccionados(paquetesObj);

      // Obtener modelos únicos del CSV
      const modelos = Array.from(new Set(clientesData.map(cliente => cliente.modelo)))
        .filter(modelo => modelo)
        .sort();
      setModelosDisponibles(modelos);

      // Inicializar checkboxes de modelos
      const modelosObj: { [key: string]: boolean } = {};
      modelos.forEach(modelo => {
        modelosObj[modelo] = true;
      });
      setModelosSeleccionados(modelosObj);

      // Obtener años únicos del CSV
      const años = Array.from(new Set(clientesData.map(cliente => cliente.año.toString())))
        .filter(año => año)
        .sort((a, b) => Number(b) - Number(a));
      setAñosDisponibles(años);

      // Inicializar checkboxes de años
      const añosObj: { [key: string]: boolean } = {};
      años.forEach(año => {
        añosObj[año] = true;
      });
      setAñosSeleccionados(añosObj);

      // Obtener asesores APS únicos del CSV
      const asesores = Array.from(new Set(clientesData.map(cliente => cliente.aps)))
        .filter((asesor): asesor is string => asesor !== undefined && asesor !== null)
        .sort();
      setAsesoresDisponibles(asesores);

      // Inicializar checkboxes de asesores APS
      const apsObj: APSType = {};
      asesores.forEach(asesor => {
        apsObj[asesor] = true;
      });
      setAPSSeleccionados(apsObj);

      // CÓDIGO ACTUALIZADO: Calcular el máximo valor de días sin venir con margen de seguridad
      const diasSinVenirArray = clientesData
        .map(cliente => cliente.diasSinVenir || 0)
        .filter(dias => !isNaN(dias) && isFinite(dias) && dias > 0);

      if (diasSinVenirArray.length > 0) {
        const minDias = Math.min(...diasSinVenirArray);
        const maxDias = Math.max(...diasSinVenirArray);
        // Si encontramos valores válidos, actualizamos los estados
        if (maxDias > 0 && isFinite(maxDias)) {
          console.log(`Valor mínimo de días sin venir encontrado: ${minDias}`);
          console.log(`Valor máximo de días sin venir encontrado: ${maxDias}`);

          // Usamos exactamente el valor máximo encontrado
          setMaxDiasSinVenirCalculado(maxDias);

          // Actualizamos los valores de los filtros
          if (minDiasSinVisita === 0) {
            setMinDiasSinVisita(minDias);
          }

          if (maxDiasSinVisita === 10000 || maxDiasSinVisita === 4800) {
            setMaxDiasSinVisita(maxDias);
          }

          console.log(`Actualizando filtro a rango ${minDias}-${maxDias}`);
        }
      } else {
        console.warn("No se encontraron valores válidos para diasSinVenir en los datos");
        // Si no hay valores válidos, establecer un valor predeterminado alto
        setMaxDiasSinVenirCalculado(10000);
        setMaxDiasSinVisita(10000);
      }
    }
  }, [clientesData, maxDiasSinVisita]); // Mantenemos maxDiasSinVisita en las dependencias

  // Inicializar el estado de los paquetes seleccionados
  const [paquetesSeleccionados, setPaquetesSeleccionados] = useState<PaquetesType>({});

  // Estado para los modelos seleccionados - inicializado dinámicamente
  const [modelosSeleccionados, setModelosSeleccionados] = useState<{ [key: string]: boolean }>({});

  // Estado para los años seleccionados - inicializado dinámicamente
  const [añosSeleccionados, setAñosSeleccionados] = useState<{ [key: string]: boolean }>({});

  // Función optimizada para detectar filtros activos
  const hayFiltrosActivos = (): boolean => {
    // Comprobar filtros directos de manera rápida
    if (searchTerm.trim() !== '' ||
      filtroNombreFactura.trim() !== '' ||
      filtroCelular.trim() !== '' ||
      fechaInicio !== null ||
      fechaFin !== null ||
      minDiasSinVisita > 0 ||
      maxDiasSinVisita < 4800) {
      return true;
    }

    // Comprobar filtros de checkboxes
    // Uso de Array.some() para optimizar la detección de filtros no seleccionados
    if (Object.values(agenciasSeleccionadas).some(v => !v) ||
      Object.values(modelosSeleccionados).some(v => !v) ||
      Object.values(añosSeleccionados).some(v => !v) ||
      Object.values(paquetesSeleccionados).some(v => !v) ||
      Object.values(apsSeleccionados).some(v => !v)) {
      return true;
    }

    return false;
  };

  // Función handlePageChange mejorada
  const handlePageChange = (newPage: number) => {
    // Evitar procesamiento si ya está cargando
    if (cargandoPagina) return;
    setIsFiltering(true);

    console.log(`Cambiando a página ${newPage}`);

    // Verificar si hay filtros activos
    const tieneFiltrosaActivos = hayFiltrosActivos();

    // Si hay filtros activos, trabajamos con los datos filtrados
    if (tieneFiltrosaActivos) {
      const totalPaginas = Math.max(1, Math.ceil(filteredData.length / itemsPerPage));

      // Verificar que la página solicitada no exceda el total de páginas disponibles
      if (newPage > totalPaginas) {
        console.log(`La página solicitada (${newPage}) excede el total disponible (${totalPaginas})`);
        // Opcionalmente podrías establecer la página al máximo disponible
        setCurrentPage(totalPaginas);
        // Importante: Restablecemos isFiltering a false
        setTimeout(() => {
          setIsFiltering(false);
        }, 300);
        return;
      }

      // Si estamos en rango, simplemente cambiamos la página
      setCurrentPage(newPage);
      // Importante: Restablecemos isFiltering a false
      setTimeout(() => {
        setIsFiltering(false);
      }, 300);
      return;
    }

    // Si no hay filtros activos, simplemente cambiamos la página
    // ya que todos los datos ya están cargados
    setCurrentPage(newPage);
    // Importante: Restablecemos isFiltering a false
    setTimeout(() => {
      setIsFiltering(false);
    }, 900);
  };

  const resetearFiltros = () => {
    setIsFiltering(true);
    // Resetear filtro de búsqueda por serie
    setSearchTerm('');

    // Resetear filtro de nombre de factura
    setFiltroNombreFactura('');

    // Resetear filtro de celular
    setFiltroCelular('');

    // Resetear rango de días sin visita
    setMinDiasSinVisita(0);
    setMaxDiasSinVisita(maxDiasSinVenirCalculado); // Usar el valor máximo calculado

    // Resetear fechas
    setFechaInicio(null);
    setFechaFin(null);

    // Resetear paginación
    setCurrentPage(1);

    // Resetear filtros de checkboxes
    // Agencias
    const resetAgencias: AgenciasType = {};
    agenciasDisponibles.forEach(agencia => {
      resetAgencias[agencia] = true;
    });
    setAgenciasSeleccionadas(resetAgencias);

    // Modelos
    const resetModelos: { [key: string]: boolean } = {};
    modelosDisponibles.forEach(modelo => {
      resetModelos[modelo] = true;
    });
    setModelosSeleccionados(resetModelos);

    // Años
    const resetAños: { [key: string]: boolean } = {};
    añosDisponibles.forEach(año => {
      resetAños[año] = true;
    });
    setAñosSeleccionados(resetAños);

    // Paquetes
    const resetPaquetes: PaquetesType = {};
    paquetesDisponibles.forEach(paquete => {
      resetPaquetes[paquete] = true;
    });
    setPaquetesSeleccionados(resetPaquetes);

    // APS
    const resetAPS: APSType = {};
    asesoresDisponibles.forEach(aps => {
      resetAPS[aps] = true;
    });
    setAPSSeleccionados(resetAPS);

    // Cerrar cualquier filtro que esté abierto
    cerrarTodosFiltros();
  };


  // 1. Añadir al useMemo de filteredData el filtro por días sin visita

  const filteredData = useMemo(() => {
    console.time('Filtrado');

    // Mostrar loader mientras se filtra
    setIsFiltering(true);

    // Variables para contar rechazos por tipo de filtro
    let rechazadosPorSerie = 0;
    let rechazadosPorFecha = 0;
    let rechazadosPorAgencia = 0;
    let rechazadosPorModelo = 0;
    let rechazadosPorAño = 0;
    let rechazadosPorPaquete = 0;
    let rechazadosPorAPS = 0;
    let rechazadosPorNombreFactura = 0;
    let rechazadosPorCelular = 0;
    let rechazadosPorDiasSinVisita = 0;

    // Lógica para buscar por múltiples series
    const seriesABuscar = searchTerm
      .split(',')
      .map(term => term.trim().toLowerCase())
      .filter(term => term.length > 0);

    // Pre-comprobación para filtros comunes para mejorar rendimiento
    const filtrarPorSerie = seriesABuscar.length > 0;
    const filtrarPorFecha = fechaInicio !== null && fechaFin !== null;
    const filtrarPorDiasSinVisita = minDiasSinVisita > 0 || maxDiasSinVisita < 4800;
    const filtrarPorNombreFactura = filtroNombreFactura.trim() !== '';
    const filtrarPorCelular = filtroCelular.trim() !== '';

    // Determinar si hay filtros de checkbox que no están todos seleccionados
    const hayFiltroAgencia = Object.values(agenciasSeleccionadas).some(v => !v);
    const hayFiltroModelo = Object.values(modelosSeleccionados).some(v => !v);
    const hayFiltroAño = Object.values(añosSeleccionados).some(v => !v);
    const hayFiltroPaquete = Object.values(paquetesSeleccionados).some(v => !v);
    const hayFiltroAPS = Object.values(apsSeleccionados).some(v => !v);

    const result = clientesData.filter(cliente => {
      // Filtro por serie
      if (filtrarPorSerie) {
        if (seriesABuscar.length > 1) {
          const serieLower = cliente.serie.toLowerCase();
          const coincideConAlguno = seriesABuscar.some(term => serieLower.includes(term));
          if (!coincideConAlguno) {
            rechazadosPorSerie++;
            return false;
          }
        } else if (!cliente.serie.toLowerCase().includes(seriesABuscar[0])) {
          rechazadosPorSerie++;
          return false;
        }
      }

      // Filtro por días sin visita - optimizado
      if (filtrarPorDiasSinVisita) {
        if (!cliente.diasSinVenir ||
          cliente.diasSinVenir < minDiasSinVisita ||
          cliente.diasSinVenir > maxDiasSinVisita) {
          rechazadosPorDiasSinVisita++;
          return false;
        }
      }

      // Filtro por fecha - versión optimizada
      if (filtrarPorFecha) {
        if (!cliente.ultimaVisita) {
          rechazadosPorFecha++;
          return false;
        }

        const inicio = new Date(fechaInicio!.getFullYear(), fechaInicio!.getMonth(), fechaInicio!.getDate());
        const fin = new Date(fechaFin!.getFullYear(), fechaFin!.getMonth(), fechaFin!.getDate());
        const fechaCliente = new Date(
          cliente.ultimaVisita.getFullYear(),
          cliente.ultimaVisita.getMonth(),
          cliente.ultimaVisita.getDate()
        );

        if (fechaCliente < inicio || fechaCliente > fin) {
          rechazadosPorFecha++;
          return false;
        }
      }

      // Filtro por agencia - optimizado
      if (hayFiltroAgencia && cliente.agencia && !agenciasSeleccionadas[cliente.agencia]) {
        rechazadosPorAgencia++;
        return false;
      }

      // Filtro por modelo - optimizado
      if (hayFiltroModelo && !modelosSeleccionados[cliente.modelo]) {
        rechazadosPorModelo++;
        return false;
      }

      // Filtro por año - optimizado
      if (hayFiltroAño && !añosSeleccionados[cliente.año.toString()]) {
        rechazadosPorAño++;
        return false;
      }

      // Filtro por paquete - VERSIÓN CORREGIDA
      if (hayFiltroPaquete) {
        // Si un cliente no tiene paquete y hay algún filtro activo, 
        // necesitamos verificar si "null" o "" están seleccionados
        if (!cliente.paquete || cliente.paquete === 'null' || cliente.paquete === '') {
          // Si no hay una opción "Sin paquete" explícita seleccionada, rechazar este registro
          const sinPaqueteSeleccionado = paquetesSeleccionados['null'] || paquetesSeleccionados[''];
          if (!sinPaqueteSeleccionado) {
            rechazadosPorPaquete++;
            return false;
          }
        }
        // Si tiene paquete, verificar si está entre los seleccionados
        else if (!paquetesSeleccionados[cliente.paquete]) {
          rechazadosPorPaquete++;
          return false;
        }
      }

      // Filtro por APS - optimizado
      if (hayFiltroAPS && cliente.aps && !apsSeleccionados[cliente.aps]) {
        rechazadosPorAPS++;
        return false;
      }

      // Filtro por Nombre Factura - optimizado
      if (filtrarPorNombreFactura && !cliente.nombreFactura.toLowerCase().includes(filtroNombreFactura.trim().toLowerCase())) {
        rechazadosPorNombreFactura++;
        return false;
      }

      // Filtro por Celular - optimizado
      if (filtrarPorCelular) {
        const termino = filtroCelular.trim();
        const celularMatch = cliente.celular?.includes(termino) || false;
        const telefonoMatch = cliente.telefono?.includes(termino) || false;
        const oficinaMatch = cliente.tOficina?.includes(termino) || false;
        const cloudtalkMatch = determinaCloudTalk(cliente)?.includes(termino) || false;

        if (!celularMatch && !telefonoMatch && !oficinaMatch && !cloudtalkMatch) {
          rechazadosPorCelular++;
          return false;
        }
      }

      // Si pasa todos los filtros, incluir el cliente
      return true;
    });

    // Reducir verbosidad de los logs
    console.log(`Filtrado completado: ${result.length} de ${clientesData.length} registros coinciden.`);
    console.timeEnd('Filtrado');

    // Ocultar el loader después del filtrado
    // Usando un timeout más largo para asegurar que el loader se oculte después de que el filtrado se complete
    setTimeout(() => {
      setIsFiltering(false);
    }, 400); // Aumentado a 400ms para dar más tiempo

    return result;
  }, [
    clientesData,
    searchTerm,
    agenciasSeleccionadas,
    modelosSeleccionados,
    añosSeleccionados,
    paquetesSeleccionados,
    apsSeleccionados,
    filtroNombreFactura,
    filtroCelular,
    fechaInicio,
    fechaFin,
    minDiasSinVisita,
    maxDiasSinVisita
  ]);

  // Luego modifica getCurrentItems para usar el dato memoizado
  const getCurrentItems = (): Cliente[] => {
    // Si estamos cargando, mostrar un indicador
    if (isLoading || cargandoPagina) {
      return [];
    }

    // Verificar si hay filtros activos
    const tieneFiltrosaActivos = hayFiltrosActivos();

    // Si no hay filtros activos - usamos datos paginados normales
    if (!tieneFiltrosaActivos) {
      // Asegurarse de que los índices sean válidos
      const indexOfLastItem = currentPage * itemsPerPage;
      const indexOfFirstItem = indexOfLastItem - itemsPerPage;

      // Verificar si hay suficientes datos para esta página
      if (indexOfFirstItem >= clientesData.length) {
        console.warn(`Índice fuera de rango: ${indexOfFirstItem} >= ${clientesData.length}.`);
        // Usar la última página disponible en lugar de resetear
        const ultimaPagina = Math.max(1, Math.ceil(clientesData.length / itemsPerPage));
        const nuevoInicio = (ultimaPagina - 1) * itemsPerPage;
        return clientesData.slice(nuevoInicio, nuevoInicio + itemsPerPage);
      }

      return clientesData.slice(indexOfFirstItem, indexOfLastItem);
    }

    // Si hay filtros activos - trabajar con los datos filtrados
    // Los índices para la página actual
    const indexOfLastItem = currentPage * itemsPerPage;
    const indexOfFirstItem = indexOfLastItem - itemsPerPage;

    // Verificar que hay datos filtrados
    if (filteredData.length === 0) {
      return []; // No hay datos que mostrar
    }

    // Verificar que los índices son válidos
    if (indexOfFirstItem >= filteredData.length) {
      console.warn(`Índice filtrado fuera de rango: ${indexOfFirstItem} >= ${filteredData.length}.`);
      // Usar la última página disponible en lugar de resetear
      const ultimaPagina = Math.max(1, Math.ceil(filteredData.length / itemsPerPage));
      const nuevoInicio = (ultimaPagina - 1) * itemsPerPage;
      return filteredData.slice(nuevoInicio, Math.min(nuevoInicio + itemsPerPage, filteredData.length));
    }

    // Devolver los elementos para la página actual
    return filteredData.slice(indexOfFirstItem, Math.min(indexOfLastItem, filteredData.length));
  };



  // Manejador para checkboxes de APS
  

  // Función para seleccionar solamente un APS
  const handleSolamenteAPS = (aps: string) => {
    setIsFiltering(true);
    const nuevosAPS: APSType = {};
    Object.keys(apsSeleccionados).forEach(key => {
      nuevosAPS[key] = key === aps;
    });
    setAPSSeleccionados(nuevosAPS);
  };

  // Manejador para seleccionar solamente un paquete


  // Manejador para checkboxes de modelo



  // Función para manejar cambios en el input del nombre de factura
  const handleNombreFacturaChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFiltroNombreFactura(e.target.value);
  };

  const handleCelularChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFiltroCelular(e.target.value);
  };

  const handleDiasSinVisitaRangeChange = (min: number, max: number) => {
    // No mostrar el loader para cada tecla presionada, solo al aplicar el filtro

    // Si los valores son los mismos que ya tenemos, no hacer nada
    if (min === minDiasSinVisita && max === maxDiasSinVisita) {
      return;
    }

    // Caso especial: si ambos valores son 0, mostrar todos los registros
    if (min === 0 && max === 0) {
      setIsFiltering(true);
      setMinDiasSinVisita(0);
      setMaxDiasSinVisita(4800);
    } else {
      setIsFiltering(true);
      setMinDiasSinVisita(min);
      setMaxDiasSinVisita(max);
    }
  };

  // Cerrar todos los filtros
  const cerrarTodosFiltros = () => {
    setMostrarFiltroAgencia(false);
    setMostrarFiltroModelo(false);
    setMostrarFiltroAño(false);
    setMostrarFiltroPaquete(false);
    setMostrarFiltroAPS(false);
    setMostrarCalendario(false);
  };


  // Función para seleccionar solamente un modelo
  const handleSolamenteModelo = (modelo: string) => {
    const nuevosModelos: { [key: string]: boolean } = {};
    Object.keys(modelosSeleccionados).forEach(key => {
      nuevosModelos[key] = key === modelo;
    });
    setModelosSeleccionados(nuevosModelos);
  };

  // Función para seleccionar solamente un año
  const handleSolamenteAño = (año: string) => {
    const nuevosAños: { [key: string]: boolean } = {};
    Object.keys(añosSeleccionados).forEach(key => {
      nuevosAños[key] = key === año;
    });
    setAñosSeleccionados(nuevosAños);
  };



  // Función para mostrar/ocultar el filtro de modelos

  


  // Función para mostrar/ocultar el calendario
  const toggleCalendario = (e: React.MouseEvent<HTMLButtonElement>) => {
    const botonRect = e.currentTarget.getBoundingClientRect();

    // Calcular la posición óptima para el menú
    setPosicionMenu({
      top: botonRect.bottom + window.scrollY,
      left: botonRect.left + window.scrollX,
      width: botonRect.width * 2 // Hacemos el calendario más ancho
    });

    // Cerrar otros filtros si están abiertos
    cerrarTodosFiltros();

    setMostrarCalendario(!mostrarCalendario);
  };

  // Función para formatear fecha
  const formatearFecha = (fecha: Date | null): string => {
    if (!fecha) return "";
    return `${fecha.getDate()} ${['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'][fecha.getMonth()]} ${fecha.getFullYear()}`;
  };

  // Función para alternar la visualización del selector de mes de inicio
  const toggleSelectorMesInicio = () => {
    setMostrarSelectorMesInicio(!mostrarSelectorMesInicio);
    setMostrarSelectorMesFin(false);
    setSelectorAñoInicio(mesInicio.getFullYear());
  };

  // Función para alternar la visualización del selector de mes de fin
  const toggleSelectorMesFin = () => {
    setMostrarSelectorMesFin(!mostrarSelectorMesFin);
    setMostrarSelectorMesInicio(false);
    setSelectorAñoFin(mesFin.getFullYear());
  };

  // Función para cambiar el año en el selector de inicio
  const cambiarAñoInicio = (incremento: number) => {
    setSelectorAñoInicio(selectorAñoInicio + incremento);
  };

  // Función para cambiar el año en el selector de fin
  const cambiarAñoFin = (incremento: number) => {
    setSelectorAñoFin(selectorAñoFin + incremento);
  };

  // Función para seleccionar un mes en el selector de inicio
  const seleccionarMesInicio = (mes: number) => {
    setMesInicio(new Date(selectorAñoInicio, mes, 1));
    setMostrarSelectorMesInicio(false);
  };

  // Función para seleccionar un mes en el selector de fin
  const seleccionarMesFin = (mes: number) => {
    setMesFin(new Date(selectorAñoFin, mes, 1));
    setMostrarSelectorMesFin(false);
  };
  // Funciones para navegar por los meses - Calendario de inicio
  const mesAnteriorInicio = () => {
    setMesInicio(new Date(mesInicio.getFullYear(), mesInicio.getMonth() - 1, 1));
  };

  const mesSiguienteInicio = () => {
    setMesInicio(new Date(mesInicio.getFullYear(), mesInicio.getMonth() + 1, 1));
  };

  // Funciones para navegar por los meses - Calendario de fin
  const mesAnteriorFin = () => {
    setMesFin(new Date(mesFin.getFullYear(), mesFin.getMonth() - 1, 1));
  };

  const mesSiguienteFin = () => {
    setMesFin(new Date(mesFin.getFullYear(), mesFin.getMonth() + 1, 1));
  };

  // Función para seleccionar una fecha
  const esFechaEnRango = (fecha: Date): boolean => {
    if (!fechaInicio || !fechaFin) return false;

    // Creamos copias sin la hora para comparación correcta
    const inicio = new Date(fechaInicio.getFullYear(), fechaInicio.getMonth(), fechaInicio.getDate());
    const fin = new Date(fechaFin.getFullYear(), fechaFin.getMonth(), fechaFin.getDate());
    const comparar = new Date(fecha.getFullYear(), fecha.getMonth(), fecha.getDate());
    return comparar >= inicio && comparar <= fin;
  };

  const esFechaInicio = (fecha: Date): boolean => {
    if (!fechaInicio) return false;
    return fecha.getDate() === fechaInicio.getDate() &&
      fecha.getMonth() === fechaInicio.getMonth() &&
      fecha.getFullYear() === fechaInicio.getFullYear();
  };

  const esFechaFin = (fecha: Date): boolean => {
    if (!fechaFin) return false;
    return fecha.getDate() === fechaFin.getDate() &&
      fecha.getMonth() === fechaFin.getMonth() &&
      fecha.getFullYear() === fechaFin.getFullYear();
  };

  const seleccionarFecha = (dia: number, esMesInicio: boolean) => {
    setIsFiltering(true);
    const mes = esMesInicio ? mesInicio : mesFin;
    const fechaSeleccionada = new Date(mes.getFullYear(), mes.getMonth(), dia);

    // Modo seleccionando fecha de inicio
    if (seleccionandoInicio) {
      setFechaInicio(fechaSeleccionada);

      // Si ya hay una fecha de fin y es menor que la nueva fecha inicio, la eliminamos
      if (fechaFin && fechaSeleccionada > fechaFin) {
        setFechaFin(null);
      }

      setSeleccionandoInicio(false);
    }
    // Modo seleccionando fecha de fin
    else {
      // Si la fecha seleccionada es anterior a la fecha de inicio, las intercambiamos
      if (fechaInicio && fechaSeleccionada < fechaInicio) {
        setFechaFin(fechaInicio);
        setFechaInicio(fechaSeleccionada);
      } else {
        setFechaFin(fechaSeleccionada);
      }

      setSeleccionandoInicio(true);
    }
  };

  // Función para aplicar el rango de fechas seleccionado
  const aplicarFechas = () => {
    setMostrarCalendario(false);
    // No necesitamos lógica adicional, ya que los filtros utilizan directamente fechaInicio y fechaFin
  };

  const obtenerDiasVaciosInicio = (mes: Date): JSX.Element[] => {
    const primerDia = new Date(mes.getFullYear(), mes.getMonth(), 1).getDay();
    return Array.from({ length: primerDia }).map((_, i) => (
      <div key={`empty-start-${i}`} className="text-center p-1"></div>
    ));
  };

  // Función para renderizar la cuadrícula de días de un mes
  const renderizarDiasMes = (mes: Date, esCalendarioInicio: boolean): JSX.Element[] => {
    const diasVacios = obtenerDiasVaciosInicio(mes);
    const diasEnMes = new Date(mes.getFullYear(), mes.getMonth() + 1, 0).getDate();
    const diasDelMes = Array.from({ length: diasEnMes }).map((_, i) =>
      renderizarDia(i + 1, esCalendarioInicio)
    );

    return [...diasVacios, ...diasDelMes];
  };

  // Función para cancelar la selección
  const cancelarSeleccion = () => {
    setMostrarCalendario(false);
  };

  const renderizarDia = (dia: number, esCalendarioInicio: boolean) => {
    const mes = esCalendarioInicio ? mesInicio : mesFin;
    const fecha = new Date(mes.getFullYear(), mes.getMonth(), dia);

    // Determinar clases para el botón
    let claseBoton = "text-center p-1 rounded hover:bg-gray-100 text-sm ";

    // Si es la fecha de inicio, aplicar estilo especial
    if (esFechaInicio(fecha)) {
      claseBoton += "bg-blue-500 text-white hover:bg-blue-600 ";
    }
    // Si es la fecha de fin, aplicar estilo especial
    else if (esFechaFin(fecha)) {
      claseBoton += "bg-blue-500 text-white hover:bg-blue-600 ";
    }
    // Si está en el rango, aplicar estilo de rango
    else if (esFechaEnRango(fecha)) {
      claseBoton += "bg-blue-100 ";
    }


    return (
      <button
        key={`day-${dia}-${esCalendarioInicio ? 'inicio' : 'fin'}`}
        className={claseBoton}
        onClick={() => seleccionarFecha(dia, esCalendarioInicio)}
      >
        {dia}
      </button>
    );
  };


  if (isLoading) {
    return (
      <div className="min-h-screen flex flex-col bg-gray-50">
        {/* Siempre mostrar la barra de navegación, incluso durante la carga */}
        <Navbar
          agenciaActual={agenciaActual}
          onAgenciaChange={handleAgenciaChange}
          isLoading={isLoading || cambiandoAgencia}
        />

        <div className="flex-1 flex items-center justify-center">
          <div className="text-center">
            <div className="animate-spin rounded-full h-16 w-16 border-t-2 border-b-2 border-[#493F91] mx-auto mb-4"></div>
            <h2 className="text-xl font-semibold text-gray-700">Cargando datos de {agenciaActual}...</h2>
            <p className="text-gray-500">Por favor espere mientras se cargan los datos del sistema.</p>
          </div>
        </div>
      </div>
    );
  }

  if (errorCarga) {
    return (
      <div className="min-h-screen flex flex-col bg-gray-50">
        {/* Siempre mostrar la barra de navegación, incluso durante un error */}
        <Navbar
          agenciaActual={agenciaActual}
          onAgenciaChange={handleAgenciaChange}
          isLoading={isLoading || cambiandoAgencia}
        />

        <div className="flex-1 flex items-center justify-center">
          <div className="text-center max-w-lg p-6 bg-white rounded-lg shadow-md">
            <svg xmlns="http://www.w3.org/2000/svg" className="h-16 w-16 text-red-500 mx-auto mb-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
            </svg>
            <h2 className="text-xl font-semibold text-gray-700 mb-2">Error al cargar datos</h2>
            <p className="text-gray-500 mb-4">{errorCarga}</p>
            <p className="text-sm text-gray-500 mb-4">Asegúrate de que el archivo {configuracionAgencias[agenciaActual].archivo} está en la carpeta correcta y tiene el formato adecuado.</p>
            <div className="flex justify-center space-x-4">
              <button
                onClick={() => handleAgenciaChange('Gran Auto')}
                className="px-4 py-2 bg-[#493F91] text-white rounded-md text-sm font-medium hover:bg-[#493F91]"
              >
                Cambiar a Gran Auto
              </button>
              <button
                onClick={() => {
                  limpiarCacheCSV();
                  window.location.reload();
                }}
                className="px-4 py-2 bg-red-600 text-white rounded-md text-sm font-medium hover:bg-red-700"
              >
                Reintentar
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Modal overlay para cerrar el menú cuando se hace clic fuera de él */}
      <FilterLoader visible={isFiltering} />
      {(mostrarFiltroAgencia || mostrarFiltroModelo || mostrarFiltroAño || mostrarFiltroPaquete || mostrarFiltroAPS || mostrarCalendario) && (
        <div
          className="fixed inset-0 z-40"
          onClick={cerrarTodosFiltros}
        ></div>
      )}

      {/* Navbar con selector de agencias */}
      <Navbar
        agenciaActual={agenciaActual}
        onAgenciaChange={handleAgenciaChange}
        isLoading={isLoading || cambiandoAgencia}
      />

      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            {/* Logo dinámico según la agencia actual */}
            <img
              src={getAgencyLogoUrl(agenciaActual)}
              alt={`${agenciaActual} Logo`}
              className="h-10"
              onError={(e) => {
                // Si hay error al cargar la imagen, usar el logo por defecto
                e.currentTarget.src = "https://i.imgur.com/ghLRDuA.jpeg";
              }}
            />
            <div className="text-gray-600">
              <h2 className="font-medium">Business Intelligence</h2>
              <h3 className="text-sm">3.2 Extractor de BD - {agenciaActual}</h3>
            </div>
          </div>

          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2">
              <span className={`text-sm ${searchTerm ? 'text-red-600 font-medium' : 'text-gray-600'}`}>
                Serie
                {searchTerm && (
                  <span className="ml-1 bg-red-100 text-red-800 text-xs px-1 rounded">
                    Filtrado
                  </span>
                )}
              </span>
              <FiltroPorSerieAvanzado
                value={searchTerm}
                onChange={debouncedSetSearchTerm}
                onSearch={handleSearch}
                placeholder="Buscar por serie"
                className="w-60"
                historialBusquedas={historialBusquedas}
                onAddToHistory={addToSearchHistory}
              />
            </div>
            <DiasSinVisitaRangeSlider
              onRangeChange={handleDiasSinVisitaRangeChange}
              initialMin={minDiasSinVisita}
              initialMax={maxDiasSinVisita}
              absoluteMin={0}
              absoluteMax={maxDiasSinVenirCalculado} // Usamos el valor calculado
            />
          </div>
        </div>
      </header>

      {/* Filters */}
      <div className="bg-white px-6 py-4 space-y-4">
        <div className="grid grid-cols-8 gap-4">
          <div className="relative">
            <FormControl fullWidth size="small">
              <InputLabel id="agencias-select-label">Agencias</InputLabel>
              <Select
                labelId="agencias-select-label"
                id="agencias-select"
                multiple
                open={mostrarFiltroAgencia} // Controlamos manualmente el estado abierto/cerrado
                onOpen={() => setMostrarFiltroAgencia(true)}
                onClose={() => setMostrarFiltroAgencia(false)}
                value={Object.keys(agenciasSeleccionadas).filter(agencia => agenciasSeleccionadas[agencia])}
                onChange={(event) => {
                  const selectedValues = event.target.value as string[];
                  const nuevoEstado = { ...agenciasSeleccionadas };

                  // Actualizar solo las agencias seleccionadas
                  Object.keys(nuevoEstado).forEach(key => {
                    nuevoEstado[key] = selectedValues.includes(key);
                  });

                  setIsFiltering(true);
                  setAgenciasSeleccionadas(nuevoEstado);
                }}
                input={<OutlinedInput label="Agencias" />}
                renderValue={(selected) => `${(selected as string[]).length} de ${agenciasDisponibles.length}`}
                MenuProps={{
                  PaperProps: {
                    style: {
                      maxHeight: 300,
                    },
                  },
                  // Importante: Evitar que se cierre al hacer clic
                  // en un elemento dentro del menú
                  keepMounted: true
                }}
              >
                {agenciasDisponibles.map((agencia) => (
                  <MenuItem key={agencia} value={agencia} sx={{ display: 'flex', justifyContent: 'space-between' }}>
                    <div style={{ display: 'flex', alignItems: 'center' }}>
                      <Checkbox checked={agenciasSeleccionadas[agencia] === true} />
                      <ListItemText primary={agencia} />
                    </div>
                    <Button
                      size="small"
                      variant="text"
                      color="primary"
                      onClick={(e) => {
                        e.stopPropagation(); // Evitar que se propague el evento de clic
                        const nuevoEstado = { ...agenciasSeleccionadas };
                        Object.keys(nuevoEstado).forEach(key => {
                          nuevoEstado[key] = key === agencia;
                        });
                        setAgenciasSeleccionadas(nuevoEstado);
                        // NO cerrar el menú aquí
                      }}
                    >
                      Solamente
                    </Button>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </div>

          <div className="relative">
            <FormControl fullWidth size="small">
              <InputLabel id="modelos-select-label">Modelo</InputLabel>
              <Select
                labelId="modelos-select-label"
                id="modelos-select"
                multiple
                open={mostrarFiltroModelo} // Controlamos manualmente el estado abierto/cerrado
                onOpen={() => setMostrarFiltroModelo(true)}
                onClose={() => setMostrarFiltroModelo(false)}
                value={Object.keys(modelosSeleccionados).filter(modelo => modelosSeleccionados[modelo])}
                onChange={(event) => {
                  const selectedValues = event.target.value as string[];
                  const nuevoEstado = { ...modelosSeleccionados };

                  // Actualizar solo los modelos seleccionados
                  Object.keys(nuevoEstado).forEach(key => {
                    nuevoEstado[key] = selectedValues.includes(key);
                  });

                  setIsFiltering(true);
                  setModelosSeleccionados(nuevoEstado);
                }}
                input={<OutlinedInput label="Modelo" />}
                renderValue={(selected) => `${(selected as string[]).length} de ${modelosDisponibles.length}`}
                MenuProps={{
                  PaperProps: {
                    style: {
                      maxHeight: 300,
                    },
                  },
                  // Importante: Evitar que se cierre al hacer clic
                  // en un elemento dentro del menú
                  keepMounted: true
                }}
              >
                {modelosDisponibles.map((modelo) => (
                  <MenuItem key={modelo} value={modelo} sx={{ display: 'flex', justifyContent: 'space-between' }}>
                    <div style={{ display: 'flex', alignItems: 'center' }}>
                      <Checkbox checked={modelosSeleccionados[modelo] === true} />
                      <ListItemText primary={modelo} />
                    </div>
                    <Button
                      size="small"
                      variant="text"
                      color="primary"
                      onClick={(e) => {
                        e.stopPropagation(); // Evitar que se propague el evento de clic
                        handleSolamenteModelo(modelo);
                      }}
                    >
                      Solamente
                    </Button>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </div>

          <div className="relative">
            <FormControl fullWidth size="small">
              <InputLabel id="anios-select-label">Año modelo</InputLabel>
              <Select
                labelId="anios-select-label"
                id="anios-select"
                multiple
                open={mostrarFiltroAño} // Controlamos manualmente el estado abierto/cerrado
                onOpen={() => setMostrarFiltroAño(true)}
                onClose={() => setMostrarFiltroAño(false)}
                value={Object.keys(añosSeleccionados).filter(año => añosSeleccionados[año])}
                onChange={(event) => {
                  const selectedValues = event.target.value as string[];
                  const nuevoEstado = { ...añosSeleccionados };

                  // Actualizar solo los años seleccionados
                  Object.keys(nuevoEstado).forEach(key => {
                    nuevoEstado[key] = selectedValues.includes(key);
                  });

                  setIsFiltering(true);
                  setAñosSeleccionados(nuevoEstado);
                }}
                input={<OutlinedInput label="Año modelo" />}
                renderValue={(selected) => `${(selected as string[]).length} de ${añosDisponibles.length}`}
                MenuProps={{
                  PaperProps: {
                    style: {
                      maxHeight: 300,
                    },
                  },
                  // Importante: Evitar que se cierre al hacer clic
                  // en un elemento dentro del menú
                  keepMounted: true
                }}
              >
                {añosDisponibles.map((año) => (
                  <MenuItem key={año} value={año} sx={{ display: 'flex', justifyContent: 'space-between' }}>
                    <div style={{ display: 'flex', alignItems: 'center' }}>
                      <Checkbox checked={añosSeleccionados[año] === true} />
                      <ListItemText primary={año} />
                    </div>
                    <Button
                      size="small"
                      variant="text"
                      color="primary"
                      onClick={(e) => {
                        e.stopPropagation(); // Evitar que se propague el evento de clic
                        handleSolamenteAño(año);
                      }}
                    >
                      Solamente
                    </Button>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </div>

          <div className="relative">
            <FormControl fullWidth size="small">
              <InputLabel id="paquetes-select-label">Paquete</InputLabel>
              <Select
                labelId="paquetes-select-label"
                id="paquetes-select"
                multiple
                open={mostrarFiltroPaquete} // Controlamos manualmente el estado abierto/cerrado
                onOpen={() => setMostrarFiltroPaquete(true)}
                onClose={() => setMostrarFiltroPaquete(false)}
                value={Object.keys(paquetesSeleccionados).filter(paquete => paquetesSeleccionados[paquete])}
                onChange={(event) => {
                  const selectedValues = event.target.value as string[];
                  const nuevoEstado = { ...paquetesSeleccionados };

                  // Actualizar solo los paquetes seleccionados
                  Object.keys(nuevoEstado).forEach(key => {
                    nuevoEstado[key] = selectedValues.includes(key);
                  });

                  setIsFiltering(true);
                  setPaquetesSeleccionados(nuevoEstado);
                }}
                input={<OutlinedInput label="Paquete" />}
                renderValue={(selected) => `${(selected as string[]).length} de ${paquetesDisponibles.length}`}
                MenuProps={{
                  PaperProps: {
                    style: {
                      maxHeight: 300,
                    },
                  },
                  // Importante: Evitar que se cierre al hacer clic
                  // en un elemento dentro del menú
                  keepMounted: true
                }}
              >
                {paquetesDisponibles.map((paquete) => (
                  <MenuItem key={paquete} value={paquete} sx={{ display: 'flex', justifyContent: 'space-between' }}>
                    <div style={{ display: 'flex', alignItems: 'center' }}>
                      <Checkbox checked={paquetesSeleccionados[paquete] === true} />
                      <ListItemText primary={paquete} />
                    </div>
                    <Button
                      size="small"
                      variant="text"
                      color="primary"
                      onClick={(e) => {
                        e.stopPropagation(); // Evitar que se propague el evento de clic
                        const nuevoEstado = { ...paquetesSeleccionados };
                        Object.keys(nuevoEstado).forEach(key => {
                          nuevoEstado[key] = key === paquete;
                        });
                        setPaquetesSeleccionados(nuevoEstado);
                      }}
                    >
                      Solamente
                    </Button>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </div>

          <div className="relative">
            <FormControl fullWidth size="small">
              <InputLabel id="aps-select-label">APS</InputLabel>
              <Select
                labelId="aps-select-label"
                id="aps-select"
                multiple
                open={mostrarFiltroAPS} // Controlamos manualmente el estado abierto/cerrado
                onOpen={() => setMostrarFiltroAPS(true)}
                onClose={() => setMostrarFiltroAPS(false)}
                value={Object.keys(apsSeleccionados).filter(aps => apsSeleccionados[aps])}
                onChange={(event) => {
                  const selectedValues = event.target.value as string[];
                  const nuevoEstado = { ...apsSeleccionados };

                  // Actualizar solo los APS seleccionados
                  Object.keys(nuevoEstado).forEach(key => {
                    nuevoEstado[key] = selectedValues.includes(key);
                  });

                  setIsFiltering(true);
                  setAPSSeleccionados(nuevoEstado);
                }}
                input={<OutlinedInput label="APS" />}
                renderValue={(selected) => `${(selected as string[]).length} de ${asesoresDisponibles.length}`}
                MenuProps={{
                  PaperProps: {
                    style: {
                      maxHeight: 300,
                    },
                  },
                  // Importante: Evitar que se cierre al hacer clic
                  // en un elemento dentro del menú
                  keepMounted: true
                }}
              >
                {asesoresDisponibles.map((aps) => (
                  <MenuItem key={aps} value={aps} sx={{ display: 'flex', justifyContent: 'space-between' }}>
                    <div style={{ display: 'flex', alignItems: 'center' }}>
                      <Checkbox checked={apsSeleccionados[aps] === true} />
                      <ListItemText primary={aps} />
                    </div>
                    <Button
                      size="small"
                      variant="text"
                      color="primary"
                      onClick={(e) => {
                        e.stopPropagation(); // Evitar que se propague el evento de clic
                        handleSolamenteAPS(aps);
                      }}
                    >
                      Solamente
                    </Button>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </div>
          <div className="relative">
            <button
              onClick={toggleCalendario}
              className={`w-full flex justify-between items-center border border-gray-300 rounded-md px-3 py-2 bg-white text-left ${(fechaInicio && fechaFin) ? 'text-red-600 font-medium' : ''
                }`}
            >
              <span className={mostrarCalendario ? "font-medium" : ""}>
                {fechaInicio && fechaFin
                  ? `${formatearFecha(fechaInicio)} - ${formatearFecha(fechaFin)}`
                  : "Período (sin filtro)"}
                {fechaInicio && fechaFin && (
                  <span className="ml-1 bg-red-100 text-red-800 text-xs px-1 rounded">
                    Filtrado
                  </span>
                )}
              </span>
              <ChevronDown className="w-4 h-4 text-gray-400" />
            </button>

            {/* Reemplaza todo el bloque de calendario con este código corregido */}
            {mostrarCalendario && (
              <div className="fixed z-50 bg-white border border-gray-300 rounded-md shadow-lg"
                style={{
                  top: `${posicionMenu.top}px`,
                  left: `${posicionMenu.left}px`,
                  width: `${posicionMenu.width}px`,
                }}
              >
                <div className="grid grid-cols-2 gap-4 p-4">
                  {/* Primera columna - Fecha inicio */}
                  <div className="relative">
                    <div className="text-center mb-2 text-sm font-medium">Fecha de inicio</div>
                    <div className="flex justify-between items-center mb-2">
                      <button
                        onClick={mesAnteriorInicio}
                        className="p-1 hover:bg-gray-100 rounded"
                        type="button"
                      >
                        &lt;
                      </button>
                      <button
                        onClick={toggleSelectorMesInicio}
                        className="text-sm font-medium hover:bg-gray-100 px-2 py-1 rounded"
                        type="button"
                      >
                        {['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'][mesInicio.getMonth()]} de {mesInicio.getFullYear()}
                      </button>
                      <button
                        onClick={mesSiguienteInicio}
                        className="p-1 hover:bg-gray-100 rounded"
                        type="button"
                      >
                        &gt;
                      </button>
                    </div>

                    {/* Selector de mes y año para inicio */}
                    {mostrarSelectorMesInicio && (
                      <div className="absolute z-10 bg-white border border-gray-200 rounded shadow-lg p-2 w-full">
                        <div className="flex justify-between items-center mb-2">
                          <button
                            onClick={() => cambiarAñoInicio(-1)}
                            className="p-1 hover:bg-gray-100 rounded"
                            type="button"
                          >
                            &lt;
                          </button>
                          <div className="text-sm font-medium">{selectorAñoInicio}</div>
                          <button
                            onClick={() => cambiarAñoInicio(1)}
                            className="p-1 hover:bg-gray-100 rounded"
                            type="button"
                          >
                            &gt;
                          </button>
                        </div>
                        <div className="grid grid-cols-3 gap-2">
                          {['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'].map((nombreMes, index) => (
                            <button
                              key={`mes-inicio-${index}`}
                              onClick={() => seleccionarMesInicio(index)}
                              className={`text-sm p-2 rounded hover:bg-gray-100 ${mesInicio.getMonth() === index && mesInicio.getFullYear() === selectorAñoInicio
                                ? 'bg-blue-100'
                                : ''
                                }`}
                              type="button"
                            >
                              {nombreMes}
                            </button>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Días de la semana */}
                    <div className="grid grid-cols-7 gap-1">
                      <div className="text-center text-xs text-gray-500">Dom</div>
                      <div className="text-center text-xs text-gray-500">Lun</div>
                      <div className="text-center text-xs text-gray-500">Mar</div>
                      <div className="text-center text-xs text-gray-500">Mié</div>
                      <div className="text-center text-xs text-gray-500">Jue</div>
                      <div className="text-center text-xs text-gray-500">Vie</div>
                      <div className="text-center text-xs text-gray-500">Sáb</div>

                      {renderizarDiasMes(mesInicio, true)}
                    </div>
                  </div>

                  {/* Segunda columna - Fecha fin */}
                  <div className="relative">
                    <div className="text-center mb-2 text-sm font-medium">Fecha de finalización</div>
                    <div className="flex justify-between items-center mb-2">
                      <button
                        onClick={mesAnteriorFin}
                        className="p-1 hover:bg-gray-100 rounded"
                        type="button"
                      >
                        &lt;
                      </button>
                      <button
                        onClick={toggleSelectorMesFin}
                        className="text-sm font-medium hover:bg-gray-100 px-2 py-1 rounded"
                        type="button"
                      >
                        {['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'][mesFin.getMonth()]} de {mesFin.getFullYear()}
                      </button>
                      <button
                        onClick={mesSiguienteFin}
                        className="p-1 hover:bg-gray-100 rounded"
                        type="button"
                      >
                        &gt;
                      </button>
                    </div>

                    {/* Selector de mes y año para fin */}
                    {mostrarSelectorMesFin && (
                      <div className="absolute z-10 bg-white border border-gray-200 rounded shadow-lg p-2 w-full">
                        <div className="flex justify-between items-center mb-2">
                          <button
                            onClick={() => cambiarAñoFin(-1)}
                            className="p-1 hover:bg-gray-100 rounded"
                            type="button"
                          >
                            &lt;
                          </button>
                          <div className="text-sm font-medium">{selectorAñoFin}</div>
                          <button
                            onClick={() => cambiarAñoFin(1)}
                            className="p-1 hover:bg-gray-100 rounded"
                            type="button"
                          >
                            &gt;
                          </button>
                        </div>
                        <div className="grid grid-cols-3 gap-2">
                          {['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'].map((nombreMes, index) => (
                            <button
                              key={`mes-fin-${index}`}
                              onClick={() => seleccionarMesFin(index)}
                              className={`text-sm p-2 rounded hover:bg-gray-100 ${mesFin.getMonth() === index && mesFin.getFullYear() === selectorAñoFin
                                ? 'bg-blue-100'
                                : ''
                                }`}
                              type="button"
                            >
                              {nombreMes}
                            </button>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Días de la semana */}
                    <div className="grid grid-cols-7 gap-1">
                      <div className="text-center text-xs text-gray-500">Dom</div>
                      <div className="text-center text-xs text-gray-500">Lun</div>
                      <div className="text-center text-xs text-gray-500">Mar</div>
                      <div className="text-center text-xs text-gray-500">Mié</div>
                      <div className="text-center text-xs text-gray-500">Jue</div>
                      <div className="text-center text-xs text-gray-500">Vie</div>
                      <div className="text-center text-xs text-gray-500">Sáb</div>

                      {renderizarDiasMes(mesFin, false)}
                    </div>
                  </div>
                </div>

                {/* Botones de acción */}
                <div className="flex justify-end p-3 border-t border-gray-200 space-x-2">
                  <button
                    onClick={cancelarSeleccion}
                    className="px-4 py-2 bg-gray-200 text-gray-700 rounded-md text-sm font-medium"
                    type="button"
                  >
                    CANCELAR
                  </button>
                  <button
                    onClick={aplicarFechas}
                    className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium"
                    type="button"
                  >
                    APLICAR
                  </button>
                </div>
              </div>
            )}
          </div>
          <div className="relative">
            <input
              type="text"
              placeholder="Nombre de factura"
              className="w-full border border-gray-300 rounded-md pl-8 pr-3 py-2 text-sm"
              value={filtroNombreFactura}
              onChange={handleNombreFacturaChange}
            />
            <Search className="absolute left-2 top-2 w-4 h-4 text-gray-400" />
          </div>

          <div className="relative">
            <input
              type="text"
              placeholder="Celular/Teléfono/Oficina"
              className="w-full border border-gray-300 rounded-md pl-8 pr-3 py-2 text-sm"
              value={filtroCelular}
              onChange={handleCelularChange}
            />
            <Search className="absolute left-2 top-2 w-4 h-4 text-gray-400" />
          </div>
        </div>
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <Calendar className="w-4 h-4 text-gray-500" />
            <span className="text-sm text-gray-600">Período:</span>
            <div className="text-sm font-medium">
              {fechaInicio && fechaFin
                ? `${formatearFecha(fechaInicio)} - ${formatearFecha(fechaFin)}`
                : "Período automático (sin filtro de fecha)"}
            </div>
          </div>
          <div className="flex space-x-3">
            <ExportCSVButton
              tableData={getCurrentItems()}
              maxRows={700}
              disabled={isLoading || cargandoPagina || filteredData.length === 0}
            />
            <Button
              variant="contained"
              onClick={resetearFiltros}
              size="small"
              sx={{
                backgroundColor: '#1976d2',
                '&:hover': { backgroundColor: '#1565c0' },
                textTransform: 'none',
                fontSize: '0.875rem',
                padding: '0.5rem 1rem',
                display: 'flex',
                alignItems: 'center',
                gap: '0.5rem'
              }}
            >
              <img
                src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADIAAAAyCAYAAAAeP4ixAAAACXBIWXMAAAsTAAALEwEAmpwYAAACn0lEQVR4nO2ZS2sUQRSFv8SJj504iK+sowgSNBgmiX/CTUQCgoguFNdqloJmqRiNf0CREGahgqILEbMQtxpXSkCN4GjizvhI4siF01AMkUzPVJUW9AfNvGrqcrpuV9U9BQUFBQU56QAOAqPAJPAaqAE/gF/AV2AGqAIXgArQSSReAM/XaLMTuAx8BOo5rw/AGNAdWkhd12psAa4DP51274Bx4BjQC2wDNgJdQBnYDxwFbgBvnf9ZHxNqE1XIMDCv31aUShWlVx4GgFvAkvqaB44QQUiX7mb2/RNgj4c4PcBDp18bnRKBhFiK3NNne4BPtTACa3EC+KYY94FNvoXY3Xmg9zWlUSj6FCMTU/Ip5JpevwD7CE+PI8ZSuW3cqdLSqZ949AGLim2TizchZ4jPScX+rOm+ZZpd2KYJxyPFsPUpuJBnhGM3sKzU3kXi3NYNs61Q0gxKyBywjoTpAGYlJubMGYSbEnKexBmRkCkS54CEvCRxtjrbJK+VYWw2OFulpsmqvvX8X0znXXwXJCRY6RmLGQmxGjtpqhJiRkHSjPosav4lFQl5E6Auj0on8F5izLJJmjEJMd8pabq1nizJDAi5NjRbwLXMhDow8yxpIWXHGjXzLDZnfQlBXmxdDqBZNLEYaDDI2xbiplgt8POS0etsk674FFKSfZmJCTkyQ/KxLNZd1ejehCBDOROzKPPMJ7bwnnbSqersvr0KyUbGPVZ4LN+pXfYCT9Xnb+Bqg3ntXUjGsDP8y/KdBnNuZ6ztId35FSdtD6/SNpgQ5MWOq2LLAs3K7RhRjV1WetjZyg6VBcc1ecw1GOQ2Cpv/EiuoEPcw9JKzN8tz2ZnjRWC778qw3Y1mv3ynKbkdC3p4vwOfgFfAHeCcRqygoKCAXPwB0TURifa7dFwAAAAASUVORK5CYII="
                alt="update-left-rotation"
                style={{ width: 20, height: 20 }}
              />
              Resetear Filtros
            </Button>

            <Button
              variant="contained"
              size="small"
              sx={{
                backgroundColor: '#1976d2', // Color azul equivalente al de los otros botones
                '&:hover': { backgroundColor: '#1565c0' },
                textTransform: 'none',
                fontSize: '0.875rem',
                padding: '0.5rem 1rem'
              }}
            >
              Buscar
            </Button>
          </div>

        </div>
      </div>
      {cargandoPagina && (
        <div className="fixed inset-0 bg-black bg-opacity-30 flex items-center justify-center z-50">
          <div className="bg-white p-4 rounded-md shadow-lg">
            <div className="animate-spin rounded-full h-8 w-8 border-t-2 border-b-2 border-red-600 mx-auto mb-2"></div>
            <p className="text-sm text-gray-700">Cargando página {currentPage + 1}...</p>
          </div>
        </div>
      )}
      {/* Table - Con el contenedor de scroll mejorado */}
      <div className="px-6 py-4">
        <div className="bg-white rounded-lg shadow">
          {/* Aplicamos un contenedor con altura fija y scroll en ambas direcciones */}
          <div className="relative" style={{ height: "500px" }}>
            {/* Tabla con cabecera fija */}
            <div className="overflow-x-auto overflow-y-auto h-full">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="sticky top-0 bg-gray-50 z-10">
                  <tr>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider sticky left-0 top-0 z-20">
                      #
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Última visita
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Serie
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Modelo
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Año
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Nombre factura
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Contacto
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Agencia
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Celular
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Teléfono
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      T. oficina
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Cloudtalk
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Paquete
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Orden
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      APS
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      TOTAL
                    </th>
                    <th className="px-6 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Días sin venir
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {getCurrentItems().length > 0 ? (
                    getCurrentItems().map((item, index) => (
                      <tr key={item.id} className={index % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 sticky left-0 bg-inherit z-10">
                          {(currentPage - 1) * itemsPerPage + index + 1}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {item.ultimaVisita ? formatearFechaTabla(item.ultimaVisita) : '-'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {item.serie}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {item.modelo}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {item.año}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {item.nombreFactura}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {item.contacto}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {item.agencia}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {!isEmpty(item.celular) ? item.celular : '-'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {!isEmpty(item.telefono) ? item.telefono : '-'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {!isEmpty(item.tOficina) ? item.tOficina : '-'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {determinaCloudTalk(item)}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {item.paquete && item.paquete !== 'null' ? item.paquete : '-'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {item.orden || '—'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {item.aps || '-'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {item.total || '—'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                          {item.diasSinVenir}
                        </td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan={17} className="px-6 py-4 text-center text-gray-500">
                        No hay datos disponibles
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
          <div className="px-6 py-3 border-t border-gray-200 bg-gray-50">
            <div className="flex items-center justify-between">
              <div className="text-sm text-gray-500">
                {filteredData.length > 0 ? (
                  <>
                    {/* Si tenemos resultados filtrados */}
                    {Math.min((currentPage - 1) * itemsPerPage + 1, filteredData.length)} - {' '}
                    {Math.min(currentPage * itemsPerPage, filteredData.length)} de {filteredData.length}

                    {/* Indicar si hay filtros activos */}
                    {hayFiltrosActivos() && (
                      <span className="ml-1 text-blue-600">
                        {/* Si toda la base está cargada */}
                        {filteredData.length !== totalRegistros ? (
                          <>(Filtrado de {clientesData.length} de {totalRegistros} totales)</>
                        ) : (
                          <>(Filtrado de {totalRegistros} totales)</>
                        )}
                      </span>
                    )}

                    {/* Si no hay filtros activos pero no tenemos todos los datos cargados */}
                    {!hayFiltrosActivos() && clientesData.length < totalRegistros && (
                      <span className="ml-1 text-blue-600">
                        (Cargados {clientesData.length} de {totalRegistros} totales)
                      </span>
                    )}
                  </>
                ) : (
                  'No hay registros que coincidan con los filtros'
                )}
              </div>
              <div className="flex items-center space-x-2">
                <button
                  className={`px-3 py-1 border border-gray-300 rounded-md text-sm ${currentPage === 1 || cargandoPagina ? 'opacity-50 cursor-not-allowed' : 'hover:bg-gray-50'
                    }`}
                  onClick={() => handlePageChange(currentPage - 1)}
                  disabled={currentPage === 1 || cargandoPagina}
                >
                  Anterior
                </button>

                {/* Mostrar número de página actual */}
                <span className="px-3 py-1 bg-red-600 text-white rounded-md text-sm">
                  {currentPage}
                </span>

                <button
                  className={`px-3 py-1 border border-gray-300 rounded-md text-sm ${cargandoPagina ||
                    // Si hay filtros activos, deshabilitar el botón cuando estemos en la última página de datos filtrados
                    (hayFiltrosActivos() && currentPage >= Math.ceil(filteredData.length / itemsPerPage)) ||
                    // Si no hay filtros, usar la lógica original
                    (!hayFiltrosActivos() && currentPage * itemsPerPage >= totalRegistros)
                    ? 'opacity-50 cursor-not-allowed'
                    : 'hover:bg-gray-50'
                    }`}
                  onClick={() => handlePageChange(currentPage + 1)}
                  disabled={
                    cargandoPagina ||
                    // Deshabilitar si hay filtros y estamos en la última página
                    (hayFiltrosActivos() && currentPage >= Math.ceil(filteredData.length / itemsPerPage)) ||
                    // Si no hay filtros, usar la lógica original
                    (!hayFiltrosActivos() && currentPage * itemsPerPage >= totalRegistros)
                  }
                >
                  {cargandoPagina ? (
                    <span className="flex items-center">
                      <svg className="animate-spin -ml-1 mr-2 h-4 w-4 text-gray-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                      Cargando...
                    </span>
                  ) : 'Siguiente'}
                </button>

                <Button
                  variant="contained"
                  className="ml-2"
                  size="small"
                  disabled={cargandoPagina}
                  onClick={() => {
                    if (cargandoPagina) return;
                    limpiarCacheCSV();
                    setClientesData([]);
                    setCurrentPage(1);
                    setIsLoading(true);
                    setTodosLosDatosCargados(false); // Forzar recarga desde el efecto
                  }}
                  sx={{
                    backgroundColor: '#1976d2', // Color azul
                    '&:hover': { backgroundColor: '#1565c0' },
                    textTransform: 'none',
                    fontSize: '0.875rem',
                    padding: '0.25rem 0.75rem', // Padding equivalente a px-3 py-1
                    marginLeft: '0.5rem' // ml-2
                  }}
                >
                  Recargar Datos de {agenciaActual}
                </Button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;