// src/utils/compressCSV.js
import { gzip, ungzip } from 'pako';
import fs from 'fs';
import path from 'path';

// Comprimir un archivo CSV
export async function compressCSV(csvFilePath, outputPath) {
  try {
    const data = fs.readFileSync(csvFilePath);
    const compressed = gzip(data);
    fs.writeFileSync(outputPath, compressed);
    console.log(`CSV comprimido: ${outputPath}`);
    return true;
  } catch (error) {
    console.error('Error al comprimir CSV:', error);
    return false;
  }
}

// Descomprimir un archivo
export async function decompressCSV(compressedPath, outputPath) {
  try {
    const compressed = fs.readFileSync(compressedPath);
    const decompressed = ungzip(compressed);
    fs.writeFileSync(outputPath, decompressed);
    console.log(`CSV descomprimido: ${outputPath}`);
    return true;
  } catch (error) {
    console.error('Error al descomprimir CSV:', error);
    return false;
  }
}