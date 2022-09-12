export const Notes = () => {
  return (
    <>
      <h3>Notes</h3>
      <ul>
        <li>
          Dates are stored in the database in UTC.
          The server queries Google's timezone API to convert them to local time, unless <code>options=gmc</code> is present.
        </li>
        <li>
          Date-times are always output as the first column, even if excluded from <strong>attributes</strong>.
        </li>
        <li>
          If you're using attributes with multiple lat/lons, be sure to include <strong>lat</strong> and <strong>lon</strong> in the attributes list.
        </li>
        <li>
          For HTML output, you can sort a column by clicking its heading.
        </li>
      </ul>
    </>
  )
}