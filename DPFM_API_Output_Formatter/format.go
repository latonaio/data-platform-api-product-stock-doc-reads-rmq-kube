package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToProductStockDoc(rows *sql.Rows) (*[]ProductStockDoc, error) {
	defer rows.Close()
	productStockDoc := make([]ProductStockDoc, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &ProductStockDoc{}

		err := rows.Scan(
			&pm.Product,
			&pm.BusinessPartner,
			&pm.Plant,
			&pm.DocType,
			&pm.DocVersionID,
			&pm.DocID,
			&pm.FileExtension,
			&pm.FileName,
			&pm.FilePath,
			&pm.DocIssuerBusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &productStockDoc, err
		}

		data := pm
		productStockDoc = append(productStockDoc, ProductStockDoc{
			Product:          		  data.Product,
			BusinessPartner:      	  data.BusinessPartner,
			Plant:               	  data.Plant,
			DocType:                  data.DocType,
			DocVersionID:             data.DocVersionID,
			DocID:                    data.DocID,
			FileExtension:            data.FileExtension,
			FileName:                 data.FileName,
			FilePath:                 data.FilePath,
			DocIssuerBusinessPartner: data.DocIssuerBusinessPartner,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &productStockDoc, nil
	}

	return &productStockDoc, nil
}
