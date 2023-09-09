package requests

type ProductStockDoc struct {
	Product				 	 string	`json:"Product"`
	BusinessPartner			 int	`json:"BusinessPartner"`
	Plant				 	 string	`json:"Plant"`
	DocType                  string `json:"DocType"`
	DocVersionID             int    `json:"DocVersionID"`
	DocID                    string `json:"DocID"`
	FileExtension            string `json:"FileExtension"`
	FileName                 string `json:"FileName"`
	FilePath                 string `json:"FilePath"`
	DocIssuerBusinessPartner int    `json:"DocIssuerBusinessPartner"`
}
