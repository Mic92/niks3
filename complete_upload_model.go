package main

const deleteUploadQuery = "DELETE FROM uploads WHERE id = :upload_id"

type completeUploadParams struct {
	UploadID int64 `db:"upload_id"`
}

func (d *DB) CompleteUpload(uploadID int64) error {
	_, err := d.deleteUploadStmt.Exec(completeUploadParams{
		UploadID: uploadID,
	})
	return err
}
