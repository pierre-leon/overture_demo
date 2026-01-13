import React, { useState } from 'react';
import './UploadControl.css';

interface UploadControlProps {
    apiUrl?: string;
    onUploadSuccess?: (data: { roadworks_count: number; enforcement_count: number }) => void;
}

export function UploadControl({ apiUrl = 'http://localhost:8000', onUploadSuccess }: UploadControlProps) {
    const [uploading, setUploading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [success, setSuccess] = useState<string | null>(null);

    const handleFileChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (!file) return;

        setUploading(true);
        setError(null);
        setSuccess(null);

        const formData = new FormData();
        formData.append('file', file);

        try {
            const response = await fetch(`${apiUrl}/upload`, {
                method: 'POST',
                body: formData,
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Upload failed');
            }

            const data = await response.json();
            setSuccess(`✓ Loaded ${data.roadworks_count} events`);

            if (onUploadSuccess) {
                onUploadSuccess(data);
            }
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Upload failed');
        } finally {
            setUploading(false);
            // Reset input
            event.target.value = '';
        }
    };

    return (
        <div className="upload-control">
            <label className="upload-button">
                <input
                    type="file"
                    accept=".parquet"
                    onChange={handleFileChange}
                    disabled={uploading}
                    style={{ display: 'none' }}
                />
                <span className="upload-button-text">
                    {uploading ? '⏳ Uploading...' : '📤 Upload Events'}
                </span>
            </label>
            {error && <div className="upload-error">{error}</div>}
            {success && <div className="upload-success">{success}</div>}
        </div>
    );
}
