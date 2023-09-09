package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-product-stock-doc-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-product-stock-doc-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var productStockDoc *[]dpfm_api_output_formatter.ProductStockDoc

	for _, fn := range accepter {
		switch fn {
		case "ProductStockDoc":
			func() {
				productStockDoc = c.ProductStockDoc(input, output, errs, log)
			}()
		}
	}

	data := &dpfm_api_output_formatter.Message{
		ProductStockDoc: 		productStockDoc,
	}

	return data
}

func (c *DPFMAPICaller) ProductStockDoc(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.ProductStockDoc {
	where := "WHERE 1 = 1"

	if input.ProductStockDoc.Product != nil {
		where = fmt.Sprintf("%s\nAND Product = \"%s\"", where, *input.ProductStockDoc.Product)
	}
	if input.ProductStockDoc.BusinessPartner != nil {
		where = fmt.Sprintf("%s\nAND BusinessPartner = %d", where, *input.ProductStockDoc.BusinessPartner)
	}
	if input.ProductStockDoc.Plant != nil {
		where = fmt.Sprintf("%s\nAND Plant = \"%s\"", where, *input.ProductStockDoc.Plant)
	}
	if input.ProductStockDoc.DocType != nil && len(*input.ProductStockDoc.DocType) != 0 {
		where = fmt.Sprintf("%s\nAND DocType = '%v'", where, *input.ProductStockDoc.DocType)
	}
	if input.ProductStockDoc.DocIssuerBusinessPartner != nil && *input.ProductStockDoc.DocIssuerBusinessPartner != 0 {
		where = fmt.Sprintf("%s\nAND DocIssuerBusinessPartner = %v", where, *input.ProductStockDoc.DocIssuerBusinessPartner)
	}
	groupBy := "\nGROUP BY Product, BusinessPartner, Plant, DocType, DocIssuerBusinessPartner "

	rows, err := c.db.Query(
		`SELECT
    	Product, BusinessPartner, Plant, DocType, MAX(DocVersionID), DocID, FileExtension, FileName, FilePath, DocIssuerBusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_product_stock_product_stock_doc_data
		` + where + groupBy + `;`)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToProductStockDoc(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}
