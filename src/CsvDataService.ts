import Papa from 'papaparse';
import { AgenciaNombre } from './components/AgenciaSelector';
import pako from 'pako';


// Interfaz ClienteCSV (sin cambios)
export interface ClienteCSV {
  ORDEN: number;
  SERIE: string;
  Modelo?: string;
  MODELO?: string;
  ANIO_VIN: number;
  NOMBRE_FAC?: string;
  NOMBRE_FACT?: string;
  CONTACTO: string;
  AGENCI?: string;
  AGENCIA?: string;
  CELULAR: string;
  TELEFONO?: string;
  OFICINA?: string;
  PAQUETE?: string;
  TOTAL?: number;
  NOMBRE_ASESOR?: string;
  ULT_VISITA?: string;
  DIAS_NOSHOW?: number;
  DIAS_SIN_VENIR?: number; // Añade este campo
  FECHA_FAC?: string;
  FECHA_FACT?: string;
}

// Definimos las interfaces para TypeScript
export interface Cliente {
  id: number;                  // ID secuencial
  serie: string;              // Número de serie del vehículo
  modelo: string;            // Modelo del vehículo
  año: number;              // Año de fabricación
  nombreFactura: string;   // Nombre del cliente para la factura                   
  contacto: string;
  agencia: string;
  celular: string;         // Campo obligatorio
  telefono?: string;       // Campo opcional
  tOficina?: string;       // Campo opcional (sin espacio ni punto)
  cloudtalk?: string;      // Campo opcional
  paquete?: string;        // Campo opcional
  orden?: number;          // Campo opcional
  total?: number;          // Campo opcional
  aps?: string;            // Campo opcional para APS
  ultimaVisita?: Date;     // Añadir esta propiedad para la fecha de última visita
  diasSinVenir: number;    // Nuevo campo para los días sin venir
}

// Configuración para cada agencia
export const configuracionAgencias = {
  'Gran Auto': {
    archivo: 'granauto.csv',
    encoding: 'cp1252',  // Codificación específica para Gran Auto
    mapeo: {
      modelo: 'Modelo',
      nombreFactura: 'NOMBRE_FAC',
      agencia: 'AGENCI',
      fechaUltimaVisita: 'ULT_VISITA'
    }
  },
  'Gasme': {
    archivo: 'gasme.csv',
    encoding: 'utf-8',
    mapeo: {
      modelo: 'MODELO',
      nombreFactura: 'NOMBRE_FAC',
      agencia: 'AGENCI',
      fechaUltimaVisita: 'FECHA_FAC'
    }
  },
  'Sierra': {
    archivo: 'sierra.csv',
    encoding: 'utf-8',
    mapeo: {
      modelo: 'MODELO',
      nombreFactura: 'NOMBRE_FAC',
      agencia: 'AGENCIA',
      // Prueba cambiando este valor
      fechaUltimaVisita: 'FECHA_FAC'
    }
  },
  'Huerpel': {
    archivo: 'huerpel.csv',
    encoding: 'utf-8',
    mapeo: {
      modelo: 'MODELO',
      nombreFactura: 'NOMBRE_FACT',
      agencia: 'AGENCIA',
      fechaUltimaVisita: 'ULT_VISITA'
    }
  },
  'Del Bravo': {
    archivo: 'delbravo.csv',
    encoding: 'utf-8',
    mapeo: {
      modelo: 'MODELO',
      nombreFactura: 'NOMBRE_FAC',
      agencia: 'AGENCIA',
      fechaUltimaVisita: 'ULT_VISITA'
    }
  }
};

// Sistema de caché para evitar recargar todo el CSV
let archivoActual: string | null = null;
let csvTextCache: string | null = null;
let clientesDataCache: Cliente[] | null = null;
let totalRegistros = 0;
let paginasServidas: Record<number, boolean> = {}; // Registro de páginas ya servidas
let todosCargados = false; // Flag para saber si ya se cargaron todos los datos

// Función para establecer la agencia actual
export const establecerAgenciaActual = (agencia: AgenciaNombre): void => {
  // Verificar que la agencia existe en la configuración
  if (!configuracionAgencias[agencia]) {
    console.error(`Agencia no válida: ${agencia}. Usando 'Gran Auto' por defecto.`);
    agencia = 'Gran Auto'; // Valor por defecto
  }

  const config = configuracionAgencias[agencia];

  // Resto del código existente...
  if (archivoActual !== config.archivo) {
    limpiarCacheCSV();
    archivoActual = config.archivo;
    console.log(`Agencia cambiada a: ${agencia}, archivo: ${config.archivo}`);
  }
};

// Nueva función mejorada para obtener una porción de los datos
// Modificada para cargar todos los datos de una vez
export const obtenerClientesPaginados = async (
  pagina: number,
  elementosPorPagina: number,
  precargaCompleta: boolean = true, // Cambiado a true por defecto
  agencia?: AgenciaNombre // Parámetro opcional para especificar la agencia
): Promise<{ clientes: Cliente[], total: number }> => {
  try {
    // Si se especifica una agencia, establecerla
    if (agencia) {
      establecerAgenciaActual(agencia);
    }

    // Si no tenemos un archivo actual configurado, usar el de Gran Auto por defecto
    if (!archivoActual) {
      establecerAgenciaActual('Gran Auto');
    }

    // Si no tenemos los datos en caché, los cargamos
    if (!clientesDataCache) {
      console.log('No hay caché, cargando datos CSV...');
      await cargarDatosCSVCompleto();
      todosCargados = true;
    }

    // Si no tenemos los datos en caché, lanzamos un error
    if (!clientesDataCache) {
      throw new Error('No se pudieron cargar los datos');
    }

    // Si ya se cargaron todos los datos o se solicita la primera página, devolvemos todo
    if (todosCargados || pagina === 1 || precargaCompleta) {
      console.log(`Devolviendo todos los datos cargados: ${clientesDataCache.length} registros`);

      // Si estamos devolviendo todos los datos, asegurémonos de marcar todas las páginas como servidas
      const totalPaginas = Math.ceil(clientesDataCache.length / elementosPorPagina);
      for (let i = 1; i <= totalPaginas; i++) {
        paginasServidas[i] = true;
      }

      return {
        clientes: clientesDataCache,
        total: totalRegistros
      };
    }

    // Este código ya no se ejecutará debido a que todosCargados será true,
    // pero lo mantenemos por compatibilidad
    const inicio = (pagina - 1) * elementosPorPagina;
    const fin = Math.min(inicio + elementosPorPagina, clientesDataCache.length);

    if (inicio >= clientesDataCache.length) {
      console.warn(`Página ${pagina} fuera de rango: inicio (${inicio}) > longitud (${clientesDataCache.length})`);

      if (clientesDataCache.length > 0) {
        const ultimaPagina = Math.ceil(clientesDataCache.length / elementosPorPagina);
        const inicioCorregido = (ultimaPagina - 1) * elementosPorPagina;
        const finCorregido = clientesDataCache.length;

        console.log(`Retornando última página ${ultimaPagina}: ${inicioCorregido}-${finCorregido}`);
        paginasServidas[ultimaPagina] = true;

        return {
          clientes: clientesDataCache.slice(inicioCorregido, finCorregido),
          total: totalRegistros
        };
      }

      return {
        clientes: [],
        total: totalRegistros
      };
    }

    const clientesPagina = clientesDataCache.slice(inicio, fin);
    paginasServidas[pagina] = true;

    console.log(`Retornando página ${pagina} con ${clientesPagina.length} registros de ${totalRegistros} totales (${inicio}-${fin})`);

    return {
      clientes: clientesPagina,
      total: totalRegistros
    };
  } catch (error) {
    console.error('Error al obtener clientes paginados:', error);
    throw error;
  }
};

export const obtenerMetadatosFiltros = async (agencia?: AgenciaNombre): Promise<{
  agencias: string[];
  modelos: string[];
  años: string[];
  paquetes: string[];
  asesores: string[];
}> => {
  try {
    // Si se especifica una agencia, establecerla
    if (agencia) {
      establecerAgenciaActual(agencia);
    }

    // Si no tenemos un archivo actual configurado, usar el de Gran Auto por defecto
    if (!archivoActual) {
      establecerAgenciaActual('Gran Auto');
    }

    // Si no tenemos los datos en caché, los cargamos
    if (!clientesDataCache) {
      console.log('No hay caché, cargando datos CSV para metadatos...');
      await cargarDatosCSVCompleto();
      todosCargados = true;
    }

    if (!clientesDataCache) {
      throw new Error('No se pudieron cargar los datos');
    }

    // Extraer todos los valores únicos para cada filtro
    const agencias = Array.from(new Set(clientesDataCache.map(cliente => cliente.agencia.trim())))
      .filter(agencia => agencia)
      .sort();

    // Extraer todos los modelos únicos
    const modelos = Array.from(new Set(clientesDataCache.map(cliente => cliente.modelo)))
      .filter(modelo => modelo)
      .sort();

    // Extraer todos los años únicos
    const años = Array.from(new Set(clientesDataCache.map(cliente => cliente.año.toString())))
      .filter(año => año)
      .sort((a, b) => Number(b) - Number(a)); // Ordenar descendente

    // Extraer todos los paquetes únicos
    const paquetes = Array.from(new Set(clientesDataCache.map(cliente =>
      cliente.paquete !== undefined ? cliente.paquete : 'null'
    )))
      .filter(paquete => paquete !== undefined)
      .sort();

    // Extraer todos los asesores APS únicos
    const asesores = Array.from(new Set(clientesDataCache.map(cliente => cliente.aps)))
      .filter((asesor): asesor is string => asesor !== undefined && asesor !== null)
      .sort();

    console.log(`Metadatos extraídos: ${agencias.length} agencias, ${modelos.length} modelos, ${años.length} años, ${paquetes.length} paquetes, ${asesores.length} asesores`);

    return {
      agencias,
      modelos,
      años,
      paquetes,
      asesores
    };
  } catch (error) {
    console.error('Error al obtener metadatos de filtros:', error);
    throw error;
  }
};

export const obtenerTodasAgencias = async (agencia?: AgenciaNombre): Promise<string[]> => {
  try {
    // Si se especifica una agencia, establecerla
    if (agencia) {
      establecerAgenciaActual(agencia);
    }

    // Si no tenemos los datos en caché, los cargamos
    if (!clientesDataCache) {
      console.log('No hay caché, cargando datos CSV para obtener todas las agencias...');
      await cargarDatosCSVCompleto();
      todosCargados = true;
    }

    if (!clientesDataCache) {
      throw new Error('No se pudieron cargar los datos');
    }

    // Extraer todas las agencias únicas (procesando el CSV completo)
    const agenciasSet = new Set<string>();
    clientesDataCache.forEach(cliente => {
      if (cliente.agencia && cliente.agencia.trim()) {
        agenciasSet.add(cliente.agencia.trim());
      }
    });

    const agencias = Array.from(agenciasSet).sort();
    console.log(`Obtenidas ${agencias.length} agencias únicas del CSV completo (${clientesDataCache.length} registros)`);

    return agencias;
  } catch (error) {
    console.error('Error al obtener todas las agencias:', error);
    return [];
  }
};

// Función para mapear de CSV a Cliente (con mejor manejo de tipos)
const mapearClienteCSVaCliente = (clienteCSV: ClienteCSV, index: number): Cliente => {
  // Obtener la configuración de la agencia actual
  const agenciaActual = Object.values(configuracionAgencias).find(config => config.archivo === archivoActual)
    || configuracionAgencias['Gran Auto'];

  // Convertir fecha de última visita a objeto Date si existe
  let ultimaVisita: Date | undefined = undefined;

  // Campo de fecha varía según la agencia
  const campoFecha = clienteCSV.FECHA_FAC || clienteCSV.FECHA_FACT || clienteCSV.ULT_VISITA;

  if (campoFecha) {
    try {
      // Verificar si es un string JSON o un string directo
      let fechaStr = campoFecha;

      // Si parece ser un objeto JSON
      if (typeof fechaStr === 'string' && (fechaStr.includes('value') || fechaStr.startsWith('{'))) {
        try {
          const fechaObj = JSON.parse(fechaStr);
          if (fechaObj.value) {
            fechaStr = fechaObj.value;
          }
        } catch (e) {
          // Si falla el parsing JSON, usar el string tal cual
          console.warn('Error al parsear JSON de fecha:', e);
        }
      }

      // Ahora procesamos la fecha según su formato
      if (typeof fechaStr === 'string') {
        if (fechaStr.includes('-')) {
          // Formato YYYY-MM-DD
          const [año, mes, dia] = fechaStr.split('-').map(num => parseInt(num, 10));
          ultimaVisita = new Date(año, mes - 1, dia); // Restamos 1 al mes
        } else if (fechaStr.includes('/')) {
          // Formato DD/MM/YYYY
          const [dia, mes, año] = fechaStr.split('/').map(num => parseInt(num, 10));
          ultimaVisita = new Date(año, mes - 1, dia);
        }
      }
    } catch (error) {
      console.error('Error al convertir fecha:', error, campoFecha);
    }
  }

  // Si la fecha no es válida, establecerla a undefined
  if (ultimaVisita && isNaN(ultimaVisita.getTime())) {
    ultimaVisita = undefined;
  }

  // Modelo puede estar en diferentes campos según la agencia
  const modelo = clienteCSV.Modelo || clienteCSV.MODELO || '';

  // Nombre de la factura puede estar en diferentes campos según la agencia
  const nombreFactura = clienteCSV.NOMBRE_FAC || clienteCSV.NOMBRE_FACT || '';

  // Agencia puede estar en diferentes campos según la agencia
  const agencia = clienteCSV.AGENCI || clienteCSV.AGENCIA || '';

  // Asegurarse de que los campos string no sean undefined
  return {
    id: index + 1,
    serie: clienteCSV.SERIE || '',
    modelo: modelo || '',
    año: clienteCSV.ANIO_VIN ? Number(clienteCSV.ANIO_VIN) : 0,
    nombreFactura: nombreFactura || '',
    contacto: clienteCSV.CONTACTO || '',
    agencia: agencia || '',
    celular: clienteCSV.CELULAR || '',
    telefono: clienteCSV.TELEFONO || undefined,
    tOficina: clienteCSV.OFICINA || undefined,
    cloudtalk: undefined,
    paquete: clienteCSV.PAQUETE !== undefined ? String(clienteCSV.PAQUETE) : undefined,
    orden: clienteCSV.ORDEN ? Number(clienteCSV.ORDEN) : undefined,
    total: clienteCSV.TOTAL ? Number(clienteCSV.TOTAL) : undefined,
    aps: clienteCSV.NOMBRE_ASESOR || '',
    ultimaVisita: ultimaVisita,
    // Esta línea ahora funcionará sin errores de TypeScript
    diasSinVenir: clienteCSV.DIAS_SIN_VENIR ? Number(clienteCSV.DIAS_SIN_VENIR) :
      (clienteCSV.DIAS_NOSHOW ? Number(clienteCSV.DIAS_NOSHOW) : 0)
  };
};

// Función para cargar los datos completos del CSV con mejor manejo de errores
// Función para cargar los datos completos del CSV con mejor manejo de errores
const cargarDatosCSVCompleto = async (intentos = 3): Promise<void> => {
  if (clientesDataCache && archivoActual === archivoActual) {
    console.log('Usando datos en caché');
    return;
  }

  try {
    // Verificar que tengamos un archivo actual seleccionado
    if (!archivoActual) {
      throw new Error('No se ha seleccionado un archivo CSV para cargar');
    }

    console.log(`Cargando archivo CSV: ${archivoActual}`);

    // Obtener la configuración de la agencia actual
    const agenciaActual = Object.entries(configuracionAgencias)
      .find(([_, config]) => config.archivo === archivoActual);

    if (!agenciaActual) {
      throw new Error(`No se encontró configuración para el archivo ${archivoActual}`);
    }

    const [nombreAgencia, config] = agenciaActual;
    console.log(`Usando configuración de agencia: ${nombreAgencia}`);

    // Intentar cargar versión comprimida primero
    const gzipPath = `${archivoActual}.gz`;
    console.log(`Intentando cargar versión comprimida: ${gzipPath}`);

    try {
      const compressedResponse = await fetch(gzipPath, {
        cache: 'no-store',
        headers: {
          'Pragma': 'no-cache',
          'Cache-Control': 'no-cache'
        }
      });

      if (compressedResponse.ok) {
        console.log("Versión comprimida encontrada, descomprimiendo...");
        // Es un archivo comprimido
        const compressedData = await compressedResponse.arrayBuffer();
        const decompressedData = pako.inflate(new Uint8Array(compressedData), { to: 'string' });
        csvTextCache = decompressedData;
        console.log("Archivo descomprimido correctamente");
      } else {
        console.log("No se encontró versión comprimida, intentando con archivo normal");
        // Intentar cargar el archivo normal
        const response = await fetch(archivoActual, {
          // Añadir opción para no usar caché del navegador
          cache: 'no-store',
          headers: {
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
          }
        });

        if (!response.ok) {
          throw new Error(`Error al cargar el archivo ${archivoActual}: ${response.status} - ${response.statusText}`);
        }

        // Almacenar el texto del CSV en caché
        csvTextCache = await response.text();
      }
    } catch (compressError) {
      console.error("Error al intentar cargar/descomprimir:", compressError);
      console.log("Intentando con archivo CSV normal...");

      // Intentar cargar el archivo normal como respaldo
      const response = await fetch(archivoActual, {
        cache: 'no-store',
        headers: {
          'Pragma': 'no-cache',
          'Cache-Control': 'no-cache'
        }
      });

      if (!response.ok) {
        throw new Error(`Error al cargar el archivo ${archivoActual}: ${response.status} - ${response.statusText}`);
      }

      // Almacenar el texto del CSV en caché
      csvTextCache = await response.text();
    }

    // Verificar que el contenido sea realmente un CSV
    if (csvTextCache.trim().startsWith('<!doctype html>') || csvTextCache.trim().startsWith('<html>')) {
      console.error('El contenido devuelto es HTML, no un CSV');
      throw new Error(`El contenido devuelto es HTML, no un CSV. Archivo: ${archivoActual}`);
    }

    // Verificar que haya contenido real
    if (csvTextCache.trim().length < 100) {
      console.error('El CSV parece estar vacío o corrupto');
      throw new Error('El CSV parece estar vacío o corrupto');
    }

    console.log('Primeros 200 caracteres del CSV:', csvTextCache.substring(0, 200));

    // Parsear el CSV con PapaParse con opciones más robustas
    const resultado = await new Promise<Papa.ParseResult<ClienteCSV>>((resolve, reject) => {
      Papa.parse<ClienteCSV>(csvTextCache!, {
        header: true,
        // IMPORTANTE: Desactivar completamente dynamicTyping para mantener strings originales
        dynamicTyping: false,
        skipEmptyLines: true,
        delimiter: '', // Auto-detectar delimitador
        transformHeader: (header) => header.trim(), // Eliminar espacios en los encabezados
        error: (error: any) => {
          console.error('Error al parsear el CSV:', error);
          reject(error);
        },
        complete: (result) => {
          if (result.errors && result.errors.length > 0) {
            console.warn('CSV parseado con errores:', result.errors);
          }
          console.log(`CSV parseado exitosamente. ${result.data.length} filas encontradas.`);
          resolve(result);
        }
      });
    });

    // Verificar si hay datos
    if (resultado.data.length === 0) {
      throw new Error('El CSV no contiene datos');
    }

    if (resultado.data.length > 0) {
      const muestra = resultado.data[0];
      console.log('Campos presentes en el primer registro:');
      console.log(Object.keys(muestra).join(', '));

      // Verificar específicamente los campos relacionados con días sin venir
      console.log('Valores de días sin venir:');
      console.log('DIAS_SIN_VENIR:', muestra.DIAS_SIN_VENIR);
      console.log('DIAS_NOSHOW:', muestra.DIAS_NOSHOW);
    }

    // Imprimir algunos registros de muestra para verificar
    console.log('Muestra de 3 registros del CSV:');
    for (let i = 0; i < Math.min(3, resultado.data.length); i++) {
      console.log(`Registro ${i + 1}:`, resultado.data[i]);
    }

    totalRegistros = resultado.data.length;
    console.log(`Total de registros en el CSV: ${totalRegistros}`);

    // Mapear los datos CSV al formato Cliente y guardar en caché
    clientesDataCache = resultado.data
      .filter(row => row.SERIE) // Filtrar filas sin serie
      .map(mapearClienteCSVaCliente);

    console.log(`Se han mapeado ${clientesDataCache.length} registros de clientes.`);

    // Establecer que todos los datos están cargados
    todosCargados = true;

    // Reiniciar el registro de páginas servidas
    paginasServidas = { 1: true };

  } catch (error) {
    console.error('Error al cargar o procesar el CSV:', error);

    // Reintento si quedan intentos disponibles
    if (intentos > 1) {
      console.log(`Reintentando cargar el CSV (quedan ${intentos - 1} intentos)...`);
      // Esperar antes de reintentar
      await new Promise(resolve => setTimeout(resolve, 2000));
      return cargarDatosCSVCompleto(intentos - 1);
    }

    // Limpiar caché si hubo error y no hay más intentos
    csvTextCache = null;
    clientesDataCache = null;
    paginasServidas = {};
    todosCargados = false;
    throw error;
  }
};

// Mantener esta función para compatibilidad con código existente
export const cargarDatosCSV = async (agencia?: AgenciaNombre): Promise<Cliente[]> => {
  try {
    console.log('Solicitud para cargar todos los datos del CSV');

    // Si se especifica una agencia, establecerla
    if (agencia) {
      establecerAgenciaActual(agencia);
    }

    // Si tenemos los datos en caché, los devolvemos directamente
    if (clientesDataCache) {
      console.log('Devolviendo datos en caché completos');
      return clientesDataCache;
    }

    // Si no hay caché, cargamos el CSV completo
    await cargarDatosCSVCompleto();

    if (!clientesDataCache) {
      throw new Error('No se pudieron cargar los datos del CSV');
    }

    return clientesDataCache;
  } catch (error) {
    console.error('Error en cargarDatosCSV:', error);
    throw error;
  }
};

// Función para limpiar la caché (útil para recargar datos)
export const limpiarCacheCSV = (): void => {
  csvTextCache = null;
  clientesDataCache = null;
  totalRegistros = 0;
  paginasServidas = {};
  todosCargados = false;
  console.log('Caché de datos CSV limpiada');
};