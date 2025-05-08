import React, { useState } from 'react';
import Papa from 'papaparse';
import Button from '@mui/material/Button';

interface ExportCSVButtonProps {
    tableData: Array<any>;
    maxRows?: number;
    filename?: string;
    disabled?: boolean;
}

const ExportCSVButton: React.FC<ExportCSVButtonProps> = ({
    tableData,
    maxRows = 700,
    filename = 'Business Intelligence',
    disabled = false
}) => {
    const [isExporting, setIsExporting] = useState<boolean>(false);

    // Función para normalizar números de teléfono (eliminar +52)
    const normalizePhone = (phone: string): string => {
        if (!phone || typeof phone !== 'string' || phone === '-') return '';

        // Eliminar cualquier prefijo +52 o 52 al inicio
        let cleanPhone = phone.replace(/^\+?52/, '');

        // Eliminar cualquier caracter que no sea un dígito
        cleanPhone = cleanPhone.replace(/\D/g, '');

        return cleanPhone;
    };

    // Función para seleccionar el primer número de teléfono disponible
    const getFirstAvailablePhone = (row: any): string => {
        // Intentar obtener de Celular, Teléfono o T. oficina en ese orden
        if (row.celular && row.celular !== '-') {
            return normalizePhone(row.celular);
        }
        if (row.telefono && row.telefono !== '-') {
            return normalizePhone(row.telefono);
        }
        if (row.tOficina && row.tOficina !== '-') {
            return normalizePhone(row.tOficina);
        }
        if (row.cloudtalk && row.cloudtalk !== '-') {
            return normalizePhone(row.cloudtalk);
        }
        return '';
    };

    // Preparar los datos para exportación según el formato requerido
    // Preparar los datos para exportación según el formato requerido
    const prepareDataForExport = (data: Array<any>): Array<any> => {
        // Limitar a máximo la cantidad de filas especificada
        const limitedData = data.slice(0, maxRows);

        return limitedData.map(row => {
            // Formatear la fecha de última visita al formato DD/MM/YYYY
            let ultimaVisitaFormateada = '';
            if (row.ultimaVisita && row.ultimaVisita instanceof Date) {
                const dia = row.ultimaVisita.getDate().toString().padStart(2, '0');
                const mes = (row.ultimaVisita.getMonth() + 1).toString().padStart(2, '0');
                const anio = row.ultimaVisita.getFullYear();
                ultimaVisitaFormateada = `${dia}/${mes}/${anio}`;
            }

            // Calcular período de inactividad en días (mostrar días sin venir)
            const periodoInactividad = row.diasSinVenir ? row.diasSinVenir.toString() : '';

            // Crear un nuevo objeto con los campos en el formato solicitado
            return {
                name: row.nombreFactura || '',
                phone: getFirstAvailablePhone(row),
                email: '',
                email2: '',
                title: '',
                company: '',
                address: '',
                city: '',
                state: '',
                zip: '',
                country: '',
                industry: '',
                website: '',
                AGENCIA: row.agencia || '',
                VIN: row.serie || '',
                MODELO: row.modelo || '',
                'AÃ\'O DEL VIN': row.año ? row.año.toString() : '',
                'ULTIMO SERVICIO': row.paquete || '',
                'ULTIMA VISITA': ultimaVisitaFormateada,
                'PERIODO DE INACTIVIDAD': periodoInactividad,
                ASESOR: row.aps || ''
            };
        });
    };

    // Función para descargar datos como CSV sin usar file-saver
    const downloadCSV = (csv: string, filename: string): void => {
        // Crear un elemento <a> para descargar
        const blob = new Blob([csv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.setAttribute('hidden', '');
        a.setAttribute('href', url);
        a.setAttribute('download', `${filename}.csv`);
        document.body.appendChild(a);

        // Trigger the download
        a.click();

        // Clean up
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    };

    // Función para manejar la exportación
    const handleExport = async () => {
        try {
            setIsExporting(true);

            // Preparar los datos para la exportación
            const dataToExport = prepareDataForExport(tableData);

            // Convertir a CSV usando PapaParse
            const csv = Papa.unparse(dataToExport, {
                header: true,
                delimiter: ',',
                newline: '\r\n', // Formato CRLF estándar para Excel
                quotes: true     // Forzar comillas en todos los campos
            });

            // Descargar el archivo
            downloadCSV(csv, filename);

        } catch (error) {
            console.error('Error al exportar datos:', error);
        } finally {
            setIsExporting(false);
        }
    };

    // Botón con estilo Material UI, mismo color que Panel Admin
    return (
        <Button
            variant="contained"
            onClick={handleExport}
            disabled={disabled || isExporting || tableData.length === 0}
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
                src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADIAAAAyCAYAAAAeP4ixAAAACXBIWXMAAAsTAAALEwEAmpwYAAABDElEQVR4nO2X/QqCQBDE5ylMev8XKf+KoBKij6fZEFY4xIv1Wrq15gdLgt6442x2AcTMDsARP4BorR6hkWAwkWgwkWgwkWgwkWgwkWiESESM1Rk0cnQL7lOM9QZ7g0YII6vXkAhNgEaCPU0wkWBPE0zkTxM5AbgAaAs0hjU9gINDHx8L9HrNPWMmp9HqmuHcOYKRBsBVr3sC2Bo0Nsmax8yaar/s78xMNZaa+PoWJWcm1SgxUWWvNWdm1Cg1UW3TmH6Jx8/0OPdS8O7DRSBNJq2lSYTYxk/NlJoI8X9kGKGbVluxD3hsLxotrN2IB0IjChNxRjhaCkfLGeFoKRwtZ4SjpUiwQim1GzcbeQG1AFqJ0C8cGAAAAABJRU5ErkJggg=="
                alt="download"
                style={{ width: 20, height: 20 }}
            />
            {isExporting ? 'Exportando...' : 'Exportar'}
        </Button>

    );
};

export default ExportCSVButton;