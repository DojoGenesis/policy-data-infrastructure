package gateway

import (
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"
)

// reGEOID matches the valid GEOID formats used in this project:
//   - 2 digits  → state
//   - 5 digits  → county
//   - 11 digits → census tract
//   - 12 digits → block group
//   - 14 digits → ward (allows extended FIPS)
//
// All digits, always zero-padded to the canonical length.
var reGEOID = regexp.MustCompile(`^\d{2}(\d{3}(\d{6}(\d{1,3})?)?)?$`)

// ValidateGEOID is a gin middleware that checks the :geoid path parameter
// format before the handler runs. It returns 400 Bad Request for malformed
// GEOIDs so handlers can assume the value is structurally valid.
func ValidateGEOID() gin.HandlerFunc {
	return func(c *gin.Context) {
		geoid := c.Param("geoid")
		if geoid == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, ErrorResponse{
				Error: "missing geoid path parameter",
			})
			return
		}
		if !reGEOID.MatchString(geoid) {
			c.AbortWithStatusJSON(http.StatusBadRequest, ErrorResponse{
				Error:  "invalid geoid format",
				Detail: "GEOIDs must be 2, 5, 11, or 12 zero-padded digits (state / county / tract / block-group)",
			})
			return
		}
		c.Next()
	}
}
