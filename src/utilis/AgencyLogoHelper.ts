// Define el tipo AgenciaNombre directamente aquí para evitar problemas de importación
export type AgenciaNombre = 'Gran Auto' | 'Gasme' | 'Sierra' | 'Huerpel' | 'Del Bravo';

// Mapeo de agencias a URLs de Imgur
const agencyLogoMap: Record<AgenciaNombre, string> = {
  'Gran Auto': 'https://i.imgur.com/AdAUsQY.jpeg',
  'Gasme': 'https://i.imgur.com/i4ISsqp.jpeg',
  'Sierra': 'https://i.imgur.com/B7xvLr6.jpeg',
  'Huerpel': 'https://imgur.com/7SnDJ1r.jpeg',
  'Del Bravo': 'https://i.imgur.com/kQzzgUg.jpeg'
};
  
// Función para obtener la URL de la imagen del logo de la agencia
export const getAgencyLogoUrl = (agencia: AgenciaNombre): string => {
  // Devolver la URL de Imgur si existe, o una imagen por defecto si no
  return agencyLogoMap[agencia] || 'https://i.imgur.com/ghLRDuA.jpeg';
};