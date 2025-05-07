// src/utils/splitCSV.js
import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';

export async function splitCSVFile(inputFile, outputPrefix, rowsPerChunk = 50000) {
  return new Promise((resolve, reject) => {
    const inputStream = fs.createReadStream(inputFile);
    
    let header = null;
    let chunkNumber = 0;
    let rowsInCurrentChunk = 0;
    let outputStream = null;
    let totalRows = 0;
    
    Papa.parse(inputStream, {
      header: true,
      step: function(result) {
        // En el primer paso, guardar el encabezado
        if (!header) {
          header = Object.keys(result.data);
        }
        
        // Si necesitamos crear un nuevo archivo de salida
        if (!outputStream || rowsInCurrentChunk >= rowsPerChunk) {
          if (outputStream) {
            outputStream.end();
          }
          
          const chunkFile = `${outputPrefix}_${chunkNumber}.csv`;
          outputStream = fs.createWriteStream(chunkFile);
          
          // Escribir encabezados
          outputStream.write(header.join(',') + '\n');
          
          rowsInCurrentChunk = 0;
          chunkNumber++;
        }
        
        // Escribir la fila actual
        const row = header.map(field => 
          typeof result.data[field] === 'string' 
            ? `"${result.data[field].replace(/"/g, '""')}"` 
            : result.data[field]
        ).join(',');
        
        outputStream.write(row + '\n');
        rowsInCurrentChunk++;
        totalRows++;
      },
      complete: function() {
        if (outputStream) {
          outputStream.end();
        }
        
        resolve({
          chunks: chunkNumber,
          totalRows: totalRows
        });
      },
      error: function(error) {
        reject(error);
      }
    });
  });
}